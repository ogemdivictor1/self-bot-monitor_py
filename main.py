import asyncio
import aiohttp
import os
import time
import random
import logging
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, jsonify
import discord
import discum
from upstash_redis import AsyncRedis, Redis as SyncRedis

load_dotenv()

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------- CONFIG --------------------
BASE_POLL_INTERVAL_SEC = 120
MAX_POLL_INTERVAL_SEC = 600
GRACE_PERIOD_MS = 2 * 60 * 1000
DISCUM_TIMEOUT_SEC = 25
GUILD_STAGGER_SEC = 0.5

NOTIFY_URL = os.getenv("NOTIFY_URL")
USER_TOKEN = os.getenv("USER_TOKEN")

if not USER_TOKEN or not NOTIFY_URL:
    logger.error("Missing USER_TOKEN or NOTIFY_URL in environment")
    exit(1)

# -------------------- REDIS --------------------
async_redis = AsyncRedis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)
sync_redis = SyncRedis(
    url=os.getenv("UPSTASH_REDIS_REST_URL"),
    token=os.getenv("UPSTASH_REDIS_REST_TOKEN")
)

# -------------------- FLASK --------------------
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

# -------------------- DISCUM SCRAPER --------------------
async def scrape_members_discum(guild_id: int, channel_id: int, token: str) -> set:
    member_ids = set()
    stop_event = threading.Event()

    def run_discum():
        bot = None
        try:
            bot = discum.Client(token=token, log=False)

            @bot.gateway.command
            def on_ready_supplemental(resp):
                if resp.event.ready_supplemental:
                    bot.gateway.fetchMembers(guild_id, channel_id, reset=True)

            @bot.gateway.command
            def on_member_chunk(resp):
                if resp.event.guild_members_chunk:
                    for member in resp.parsed.auto():
                        try:
                            member_ids.add(str(member['user']['id']))
                        except (KeyError, TypeError):
                            continue

            bot.gateway.run(auto_reconnect=False)
        except Exception as e:
            logger.error(f"Discum error for guild {guild_id}: {e}")
        finally:
            if bot:
                try:
                    bot.gateway.close()
                except:
                    pass
            stop_event.set()

    thread = threading.Thread(target=run_discum, daemon=True)
    thread.start()

    # Wait with early exit on stability
    start = time.time()
    last_size = 0
    while time.time() - start < DISCUM_TIMEOUT_SEC:
        await asyncio.sleep(0.5)
        current_size = len(member_ids)
        if current_size == last_size and current_size > 100:
            await asyncio.sleep(1.0)
            break
        last_size = current_size

    stop_event.set()
    logger.info(f"Discum scraped {len(member_ids)} members from guild {guild_id}")
    return member_ids

# -------------------- MAIN CLIENT --------------------
class MemberMonitor(discord.Client):
    def __init__(self):
        super().__init__(self_bot=True)
        self.poll_task = None
        self.session = None
        self.start_time_ms = None
        self.current_poll_interval = BASE_POLL_INTERVAL_SEC

    async def on_ready(self):
        global client_ref
        client_ref = self
        logger.info(f"Selfbot ready → {self.user} | {len(self.guilds)} guilds")

        self.session = aiohttp.ClientSession()

        # Global start time
        start = await async_redis.get("global:start_time")
        if not start:
            self.start_time_ms = int(time.time() * 1000)
            await async_redis.set("global:start_time", self.start_time_ms)
        else:
            self.start_time_ms = int(start)

        if not self.poll_task or self.poll_task.done():
            self.poll_task = asyncio.create_task(self.poll_loop())

    async def poll_loop(self):
        await asyncio.sleep(3)
        consecutive_failures = 0

        while True:
            cycle_start = time.time()
            logger.info(f"Starting poll cycle — {len(self.guilds)} guilds")

            tasks = []
            for i, guild in enumerate(self.guilds):
                if i > 0:
                    await asyncio.sleep(GUILD_STAGGER_SEC)
                tasks.append(self.poll_guild(guild))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            fails = sum(1 for r in results if isinstance(r, Exception))

            if fails > len(self.guilds) // 2:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    self.current_poll_interval = min(MAX_POLL_INTERVAL_SEC, self.current_poll_interval * 1.5)
                    logger.warning(f"High failure rate → poll interval now {self.current_poll_interval}s")
            else:
                consecutive_failures = 0
                self.current_poll_interval = BASE_POLL_INTERVAL_SEC

            elapsed = time.time() - cycle_start
            wait = max(0, self.current_poll_interval - elapsed) + random.uniform(0, 5)
            logger.info(f"Cycle done in {elapsed:.1f}s | Next in {wait:.1f}s")
            await asyncio.sleep(wait)

    async def poll_guild(self, guild: discord.Guild):
        logger.info(f"Polling {guild.name} ({guild.id}) — {guild.member_count:,} members")

        members = {}
        try:
            if guild.member_count and guild.member_count < 5000:
                async for member in guild.fetch_members(limit=None):
                    members[str(member.id)] = {
                        'user': {'id': member.id, 'username': str(member)},
                        'joined_at': member.joined_at
                    }
            else:
                channel = next((c for c in guild.text_channels
                                if c.permissions_for(guild.me).read_messages), None)
                if channel:
                    member_ids = await scrape_members_discum(guild.id, channel.id, USER_TOKEN)
                    poll_time = datetime.now(timezone.utc)
                    for uid in member_ids:
                        members[uid] = {
                            'user': {'id': int(uid), 'username': 'unknown'},
                            'joined_at': None,
                            '_first_seen_at': poll_time
                        }
                else:
                    for member in guild.members:
                        members[str(member.id)] = {
                            'user': {'id': member.id, 'username': str(member)},
                            'joined_at': member.joined_at
                        }
                    logger.warning(f"No readable channel in {guild.name}")
        except Exception as e:
            logger.error(f"Failed polling {guild.name}: {e}")
            return

        await self.process_members(guild, members)

    async def process_members(self, guild: discord.Guild, members: dict):
        guild_key = f"guild:{guild.id}:members"
        is_first_scan = await async_redis.exists(guild_key) == 0
        new_ids = []
        notifications = []
        effective_start = self.start_time_ms - GRACE_PERIOD_MS
        poll_time_ms = int(time.time() * 1000)

        for user_id, data in members.items():
            is_known = await async_redis.sismember(guild_key, user_id)
            if not is_known:
                new_ids.append(user_id)
                joined_at = data.get('joined_at')
                if joined_at:
                    joined_ms = int(joined_at.timestamp() * 1000)
                elif '_first_seen_at' in data:
                    joined_ms = int(data['_first_seen_at'].timestamp() * 1000)
                else:
                    joined_ms = poll_time_ms

                if not is_first_scan and joined_ms > effective_start:
                    notifications.append({
                        "server": guild.name,
                        "serverId": str(guild.id),
                        "userId": user_id,
                        "username": data.get('user', {}).get('username', 'Unknown'),
                        "joinedAt": datetime.fromtimestamp(joined_ms/1000, timezone.utc).isoformat(),
                        "source": "poll"
                    })

        if new_ids:
            await async_redis.sadd(guild_key, *new_ids)

        if notifications:
            await asyncio.gather(*[self.send_notification(p) for p in notifications], return_exceptions=True)

        logger.info(f"{guild.name}: {len(members)} total | {len(new_ids)} new | {len(notifications)} notified")

    async def send_notification(self, payload: dict):
        for attempt in range(3):
            try:
                async with self.session.post(NOTIFY_URL, json=payload, timeout=8) as resp:
                    if resp.status == 200:
                        logger.info(f"Notified: {payload.get('username')} → {payload.get('server')}")
                        return
            except Exception as e:
                if attempt == 2:
                    logger.error(f"Notification failed: {e}")
                await asyncio.sleep(0.6 * (attempt + 1))

    async def close(self):
        if self.session:
            await self.session.close()
        await super().close()

# -------------------- START --------------------
if __name__ == "__main__":
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()
    logger.info("Flask health server started on port 3000")

    client = MemberMonitor()
    try:
        client.run(USER_TOKEN, bot=False)
    except discord.LoginFailure:
        logger.error("TOKEN EXPIRED OR INVALID")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Unexpected crash: {e}")
