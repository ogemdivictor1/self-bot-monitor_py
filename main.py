import asyncio
import aiohttp
import os
import time
import threading
import discord
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify
import discum
from upstash_redis import AsyncRedis, Redis as SyncRedis

load_dotenv()

# -------------------- REDIS (ASYNC for bot, SYNC for Flask) --------------------
async_redis = AsyncRedis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)
sync_redis = SyncRedis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)

# -------------------- CONFIG (Hardcoded for 2‑minute detection) --------------------
POLL_INTERVAL_SEC = 120          # Poll every 2 minutes
GRACE_PERIOD_MS = 2 * 60 * 1000  # 2 minutes grace period
DISCUM_TIMEOUT_SEC = 25          # Max time to wait for discum chunks
GUILD_STAGGER_SEC = 0.5          # Delay between starting each guild poll
NOTIFY_URL = os.getenv("NOTIFY_URL")
USER_TOKEN = os.getenv("USER_TOKEN")
START_TIME = None

print(f"⚙️ Configuration: Poll every {POLL_INTERVAL_SEC}s, Grace period {GRACE_PERIOD_MS/1000}s")

# -------------------- FLASK HEALTH SERVER --------------------
app = Flask(__name__)
client_ref = None

@app.route("/")
def health():
    guilds = len(client_ref.guilds) if client_ref else 0
    return jsonify({
        "status": "running",
        "guilds": guilds,
        "uptime": int(time.time()),
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

@app.route("/stats")
def stats():
    if not client_ref:
        return jsonify({"error": "bot not ready"})
    guild_stats = []
    for guild in client_ref.guilds:
        key = f"guild:{guild.id}:members"
        try:
            count = sync_redis.scard(key)
        except Exception:
            count = 0
        guild_stats.append({
            "name": guild.name,
            "id": str(guild.id),
            "memberCount": guild.member_count,
            "trackedInRedis": count or 0
        })
    return jsonify({
        "totalGuilds": len(client_ref.guilds),
        "guilds": guild_stats,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })

def run_flask():
    app.run(host="0.0.0.0", port=3000, use_reloader=False, threaded=True)

# -------------------- DISCUM GATEWAY SCRAPER (Optimized) --------------------
async def scrape_members_discum(guild_id, channel_id, token):
    """
    Scrape members using discum's gateway lazy loading.
    Returns a set of member IDs (strings).
    Optimized: shorter timeout, immediate closure after collection.
    """
    member_ids = set()
    event_ready = threading.Event()
    stop_scraping = threading.Event()
    
    def run_discum():
        try:
            bot = discum.Client(token=token, log=False)
            
            @bot.gateway.command
            def handle_response(resp):
                if resp.event.ready_supplemental:
                    event_ready.set()
                    bot.gateway.fetchMembers(guild_id, channel_id)
                
                if resp.event.guild_members_chunk:
                    for member in resp.parsed.auto():
                        try:
                            uid = str(member['user']['id'])
                            member_ids.add(uid)
                        except (KeyError, TypeError):
                            pass
            
            bot.gateway.run(auto_reconnect=False, stop_event=stop_scraping)
        except Exception as e:
            print(f"[DISCUM ERROR] {e}")
        finally:
            try:
                bot.gateway.close()
            except:
                pass
    
    thread = threading.Thread(target=run_discum, daemon=True)
    thread.start()
    
    if not event_ready.wait(timeout=10):
        print(f"[DISCUM] Gateway not ready for guild {guild_id}")
        stop_scraping.set()
        return set()
    
    # Wait for chunks up to DISCUM_TIMEOUT_SEC, but stop early if no new members for 3 seconds
    start = time.time()
    last_size = 0
    stable_count = 0
    while (time.time() - start) < DISCUM_TIMEOUT_SEC:
        await asyncio.sleep(0.3)
        if len(member_ids) == last_size:
            stable_count += 1
            if stable_count >= 10:  # 3 seconds no change
                break
        else:
            stable_count = 0
            last_size = len(member_ids)
    
    stop_scraping.set()
    print(f"[DISCUM] Fetched {len(member_ids)} members from guild {guild_id}")
    return member_ids

# -------------------- DISCORD CLIENT (Optimized) --------------------
intents = discord.Intents.default()
intents.members = True
intents.guilds = True

class MemberMonitor(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.poll_task = None
        self.session = None  # shared aiohttp session for notifications

    async def on_ready(self):
        global client_ref, START_TIME
        client_ref = self
        print(f"🤖 Selfbot: {self.user} | {len(self.guilds)} servers")
        
        # Create shared aiohttp session for notifications
        self.session = aiohttp.ClientSession()
        
        # Get or create global start time
        start = await async_redis.get("global:start_time")
        if not start:
            START_TIME = int(time.time() * 1000)
            await async_redis.set("global:start_time", START_TIME)
            print(f"🕒 First run – start time = {datetime.fromtimestamp(START_TIME/1000, tz=timezone.utc).isoformat()}")
        else:
            START_TIME = int(start)
            print(f"🕒 Existing start time = {datetime.fromtimestamp(START_TIME/1000, tz=timezone.utc).isoformat()}")
        
        # Start polling loop immediately
        if not self.poll_task or self.poll_task.done():
            self.poll_task = self.loop.create_task(self.poll_loop())

    async def on_guild_remove(self, guild):
        await async_redis.delete(f"guild:{guild.id}:members")
        print(f"➖ Left: {guild.name}")

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

    async def poll_loop(self):
        """Main polling loop – every POLL_INTERVAL_SEC seconds"""
        await asyncio.sleep(2)  # brief wait for initial cache
        while True:
            cycle_start = time.time()
            print(f"\n🔄 Starting poll cycle ({len(self.guilds)} guilds) at {datetime.now().isoformat()}")
            
            # Launch all guild polls concurrently with staggered starts
            tasks = []
            for i, guild in enumerate(self.guilds):
                if i > 0:
                    await asyncio.sleep(GUILD_STAGGER_SEC)
                tasks.append(self.loop.create_task(self.poll_guild(guild)))
            
            # Wait for all polls to complete
            await asyncio.gather(*tasks, return_exceptions=True)
            
            elapsed = time.time() - cycle_start
            print(f"✅ Poll cycle completed in {elapsed:.1f}s. Next cycle in {POLL_INTERVAL_SEC}s")
            
            wait_time = max(0, POLL_INTERVAL_SEC - elapsed)
            await asyncio.sleep(wait_time)

    async def poll_guild(self, guild):
        """Poll a single guild for new members"""
        print(f"🔍 Polling {guild.name} (ID {guild.id})...")
        poll_start = time.time()
        members = {}

        try:
            # Small servers: use discord.py-self fetch (has joined_at)
            if guild.member_count and guild.member_count < 5000:
                async for member in guild.fetch_members(limit=None):
                    members[str(member.id)] = {
                        'user': {'id': member.id, 'username': str(member)},
                        'joined_at': member.joined_at
                    }
                print(f"  ✓ Fetched {len(members)} members via API")
            
            # Large servers: use discum gateway scraping
            elif guild.member_count and guild.member_count >= 5000:
                channel = next(
                    (c for c in guild.text_channels 
                     if c.permissions_for(guild.me).read_messages),
                    None
                )
                if channel:
                    member_ids = await scrape_members_discum(guild.id, channel.id, USER_TOKEN)
                    for uid in member_ids:
                        members[uid] = {
                            'user': {'id': int(uid), 'username': 'unknown'},
                            'joined_at': None
                        }
                    print(f"  ✓ Scraped {len(members)} members via discum")
                else:
                    print(f"  ⚠️ No readable channel – using cache")
                    for member in guild.members:
                        members[str(member.id)] = {
                            'user': {'id': member.id, 'username': str(member)},
                            'joined_at': member.joined_at
                        }
            else:
                # Fallback: use local cache
                for member in guild.members:
                    members[str(member.id)] = {
                        'user': {'id': member.id, 'username': str(member)},
                        'joined_at': member.joined_at
                    }
                print(f"  ✓ Using cache: {len(members)} members")

        except asyncio.TimeoutError:
            print(f"  ❌ Timeout for {guild.name}")
        except Exception as e:
            print(f"  ❌ Error: {e}")

        if not members:
            return

        await self.process_members(guild, members)
        print(f"  Done in {time.time() - poll_start:.1f}s")

    async def process_members(self, guild, members):
        """Compare members against Redis and send notifications"""
        guild_key = f"guild:{guild.id}:members"
        
        try:
            is_first_scan = await async_redis.exists(guild_key) == 0
        except Exception:
            is_first_scan = True

        new_ids = []
        notifications = []
        effective_start = START_TIME - GRACE_PERIOD_MS
        poll_time = int(time.time() * 1000)

        for user_id, member_data in members.items():
            try:
                is_known = await async_redis.sismember(guild_key, user_id)
            except Exception:
                is_known = False

            if not is_known:
                new_ids.append(user_id)
                joined_at = member_data.get('joined_at')
                username = member_data.get('user', {}).get('username', 'Unknown')
                
                if joined_at:
                    joined_ms = int(joined_at.timestamp() * 1000)
                else:
                    joined_ms = poll_time
                
                if not is_first_scan and joined_ms > effective_start:
                    notifications.append({
                        "server": guild.name,
                        "serverId": str(guild.id),
                        "userId": user_id,
                        "username": username,
                        "joinedAt": joined_at.isoformat() if joined_at else datetime.now(timezone.utc).isoformat(),
                        "source": "poll"
                    })
                # else: silently add to Redis later

        if new_ids:
            try:
                await async_redis.sadd(guild_key, *new_ids)
            except Exception as e:
                print(f"  Redis error: {e}")

        if notifications:
            await asyncio.gather(*[self.send_notification(p) for p in notifications])

        print(f"  {guild.name}: {len(members)} total, {len(new_ids)} new, {len(notifications)} notified")

    async def send_notification(self, payload, max_retries=2):
        """Send notification using shared aiohttp session"""
        for i in range(max_retries):
            try:
                async with self.session.post(NOTIFY_URL, json=payload, timeout=5) as resp:
                    if resp.status == 200:
                        print(f"  ✅ Notified: {payload['username']} joined {payload['server']}")
                        return
            except Exception as e:
                if i == max_retries - 1:
                    print(f"  ❌ Notify failed: {e}")
                else:
                    await asyncio.sleep(0.5)

# -------------------- MAIN --------------------
if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("✅ Health server on port 3000")

    monitor = MemberMonitor(intents=intents)

    try:
        monitor.run(USER_TOKEN, bot=False)
    except discord.LoginFailure:
        print("===============================================")
        print("🔴 TOKEN EXPIRED – update USER_TOKEN")
        print("===============================================")
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
