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
from upstash_redis import Redis

load_dotenv()

# -------------------- REDIS --------------------
redis = Redis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)

# -------------------- CONFIG --------------------
POLL_INTERVAL_SEC = 600
GRACE_PERIOD_MS = 5 * 60 * 1000
NOTIFY_URL = os.getenv("NOTIFY_URL")
USER_TOKEN = os.getenv("USER_TOKEN")
START_TIME = None

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
            count = redis.scard(key)
        except:
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
    app.run(host="0.0.0.0", port=3000)

# -------------------- DISCUM GATEWAY SCRAPER --------------------
async def scrape_members_discum(guild_id, channel_id, token, timeout=30):
    """
    Scrape members using discum's gateway lazy loading.
    Returns a set of member IDs (strings).
    No joined_at data available from gateway chunks.
    """
    member_ids = set()
    event_ready = threading.Event()
    chunks_received = threading.Event()
    
    def run_discum():
        try:
            bot = discum.Client(token=token, log=False)
            chunks_count = 0
            
            @bot.gateway.command
            def handle_response(resp):
                nonlocal chunks_count
                
                if resp.event.ready_supplemental:
                    print(f"[DISCUM] Gateway ready, fetching members for guild {guild_id}...")
                    event_ready.set()
                    bot.gateway.fetchMembers(guild_id, channel_id)
                
                if resp.event.guild_members_chunk:
                    chunks_count += 1
                    for member in resp.parsed.auto():
                        try:
                            uid = str(member['user']['id'])
                            member_ids.add(uid)
                        except (KeyError, TypeError):
                            pass
                    print(f"[DISCUM] Chunk {chunks_count}: +{len(resp.parsed.auto() or [])} members (total {len(member_ids)})")
                    
                    # Signal that we've received at least one chunk
                    if not chunks_received.is_set():
                        chunks_received.set()
            
            # Run gateway with timeout
            try:
                bot.gateway.run()
            except Exception as e:
                print(f"[DISCUM GATEWAY ERROR] {e}")
            finally:
                try:
                    bot.gateway.close()
                except:
                    pass
        
        except Exception as e:
            print(f"[DISCUM INIT ERROR] {e}")
    
    # Run discum in a separate thread (non-blocking)
    thread = threading.Thread(target=run_discum, daemon=True)
    thread.start()
    
    # Wait for gateway ready (max 10 seconds)
    ready = event_ready.wait(timeout=10)
    if not ready:
        print(f"[DISCUM] Gateway not ready for guild {guild_id}")
        return set()
    
    print(f"[DISCUM] Waiting for member chunks...")
    
    # Wait for chunks to arrive (up to `timeout` seconds)
    start = time.time()
    while (time.time() - start) < timeout:
        if chunks_received.is_set() and len(member_ids) > 0:
            # Give it a final moment to collect more chunks
            await asyncio.sleep(2)
            break
        await asyncio.sleep(0.5)
    
    print(f"[DISCUM] Completed: {len(member_ids)} member IDs from guild {guild_id}")
    return member_ids

# -------------------- DISCORD CLIENT --------------------
intents = discord.Intents.default()
intents.members = True
intents.guilds = True

class MemberMonitor(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.poll_task = None

    async def on_ready(self):
        global client_ref, START_TIME
        client_ref = self
        print(f"🤖 Selfbot: {self.user} | {len(self.guilds)} servers")

        start = redis.get("global:start_time")
        if not start:
            START_TIME = int(time.time() * 1000)
            redis.set("global:start_time", START_TIME)
            print(f"🕒 First run – start time = {datetime.fromtimestamp(START_TIME/1000, tz=timezone.utc).isoformat()}")
        else:
            START_TIME = int(start)
            print(f"🕒 Existing start time = {datetime.fromtimestamp(START_TIME/1000, tz=timezone.utc).isoformat()}")

        if not self.poll_task or self.poll_task.done():
            self.poll_task = self.loop.create_task(self.poll_loop())

    async def on_guild_remove(self, guild):
        redis.delete(f"guild:{guild.id}:members")
        print(f"➖ Left: {guild.name}")

    async def on_error(self, event, *args, **kwargs):
        import traceback
        traceback.print_exc()

    async def poll_loop(self):
        """Main polling loop"""
        await asyncio.sleep(5)
        while True:
            try:
                print(f"\n🔄 Starting poll cycle ({len(self.guilds)} guilds)...")
                for i, guild in enumerate(self.guilds):
                    await asyncio.sleep(i * 2)
                    self.loop.create_task(self.poll_guild(guild))
                
                await asyncio.sleep(POLL_INTERVAL_SEC)
            except Exception as e:
                print(f"[POLL LOOP ERROR] {e}")
                await asyncio.sleep(10)

    async def poll_guild(self, guild):
        """Poll a single guild for new members"""
        print(f"\n🔍 Polling {guild.name}...")
        poll_start = time.time()
        members = {}

        try:
            # Small servers: Use discord.py-self fetch (has joined_at)
            if guild.member_count and guild.member_count < 5000:
                print(f"[FETCH] {guild.name} ({guild.member_count} members) – using discord.py-self")
                async for member in guild.fetch_members(limit=None):
                    members[str(member.id)] = {
                        'user': {'id': member.id, 'username': str(member)},
                        'joined_at': member.joined_at
                    }
                print(f"[FETCH OK] {guild.name} → {len(members)} members")
            
            # Large servers: Use discum gateway scraping (no joined_at)
            elif guild.member_count and guild.member_count >= 5000:
                print(f"[SCRAPE] {guild.name} ({guild.member_count} members) – using discum gateway")
                
                # Get first readable channel
                channel = next(
                    (c for c in guild.text_channels 
                     if c.permissions_for(guild.me).read_messages),
                    None
                )
                
                if channel:
                    member_ids = await scrape_members_discum(
                        guild.id,
                        channel.id,
                        USER_TOKEN,
                        timeout=30
                    )
                    
                    # Convert member IDs to dict format (no joined_at from gateway)
                    for uid in member_ids:
                        members[uid] = {
                            'user': {'id': int(uid), 'username': 'unknown'},
                            'joined_at': None
                        }
                    print(f"[SCRAPE OK] {guild.name} → {len(members)} members")
                else:
                    print(f"[SCRAPE SKIP] No readable channel in {guild.name} – using cache")
                    for member in guild.members:
                        members[str(member.id)] = {
                            'user': {'id': member.id, 'username': str(member)},
                            'joined_at': member.joined_at
                        }
            
            # Fallback: Use cache
            else:
                print(f"[CACHE] {guild.name} – using local member cache")
                for member in guild.members:
                    members[str(member.id)] = {
                        'user': {'id': member.id, 'username': str(member)},
                        'joined_at': member.joined_at
                    }

        except asyncio.TimeoutError:
            print(f"[TIMEOUT] {guild.name}")
        except Exception as e:
            print(f"[FETCH ERROR] {guild.name}: {e}")

        if not members:
            if guild.member_count and guild.member_count > 5000:
                print(f"[NOTE] {guild.name} is large ({guild.member_count}+ members) – Could not fetch members.")
            return

        await self.process_members(guild, members)

        elapsed = time.time() - poll_start
        print(f"[{guild.name}] done in {elapsed:.1f}s")

    async def process_members(self, guild, members):
        """Compare members against Redis and send notifications"""
        guild_key = f"guild:{guild.id}:members"
        
        try:
            is_first_scan = redis.exists(guild_key) == 0
        except:
            is_first_scan = True

        new_ids = []
        notifications = []
        effective_start = START_TIME - GRACE_PERIOD_MS
        poll_time = int(time.time() * 1000)

        for user_id, member_data in members.items():
            try:
                is_known = redis.sismember(guild_key, user_id)
            except:
                is_known = False

            if not is_known:
                new_ids.append(user_id)
                
                try:
                    joined_at = member_data.get('joined_at')
                    username = member_data.get('user', {}).get('username', 'Unknown')
                    
                    # Smart joined_at handling
                    if joined_at:
                        # Has actual join time (from discord.py-self fetch)
                        joined_ms = int(joined_at.timestamp() * 1000)
                    else:
                        # No join time (from discum gateway) - use poll time
                        joined_ms = poll_time
                        print(f"[NOTE] {username} – no join timestamp, using poll time")
                    
                    # Only notify if NOT first scan AND joined after start time
                    if not is_first_scan and joined_ms > effective_start:
                        print(f"🆕 NEW: {username} joined {guild.name}")
                        notifications.append({
                            "server": guild.name,
                            "serverId": str(guild.id),
                            "userId": user_id,
                            "username": username,
                            "joinedAt": joined_at.isoformat() if joined_at else datetime.now(timezone.utc).isoformat(),
                            "source": "poll"
                        })
                    else:
                        reason = "Baseline" if is_first_scan else "Old"
                        print(f"📦 {reason}: {username}")
                        
                except Exception as e:
                    print(f"[MEMBER PARSE ERROR] {e}")

        if new_ids:
            try:
                redis.sadd(guild_key, *new_ids)
            except Exception as e:
                print(f"[REDIS ERROR] {e}")

        for payload in notifications:
            await self.send_notification(payload)

        print(f"[{guild.name}] {'BASELINE' if is_first_scan else 'Incremental'} – {len(members)} total, {len(new_ids)} new, {len(notifications)} notified")

    async def send_notification(self, payload, max_retries=3):
        """Send notification to CYPHER XXD bot"""
        for i in range(max_retries):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        NOTIFY_URL,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            print(f"✅ Notified: {payload['username']} joined {payload['server']}")
                            return
                        else:
                            print(f"❌ Notify failed: HTTP {resp.status}")
            except Exception as e:
                if i == max_retries - 1:
                    print(f"❌ Notify failed after {max_retries} retries: {e}")
                else:
                    await asyncio.sleep(2)

# -------------------- MAIN --------------------
if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    print("✅ Health server on :3000")

    monitor = MemberMonitor(intents=intents)

    try:
        monitor.run(USER_TOKEN, bot=False)
    except discord.LoginFailure:
        print("================================")
        print("🔴 TOKEN EXPIRED – update USER_TOKEN")
        print("================================")
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
