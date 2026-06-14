import asyncio
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

load_dotenv()

# -------------------- LOGGING --------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# -------------------- CONFIG --------------------
POLL_INTERVAL_SEC = 120
DISCUM_TIMEOUT_SEC = 25
GUILD_STAGGER_SEC = 0.5

USER_TOKEN = os.getenv("USER_TOKEN")

if not USER_TOKEN:
    logger.error("Missing USER_TOKEN in environment")
    exit(1)

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
        guild_stats.append({
            "name": guild.name,
            "id": str(guild.id),
            "memberCount": guild.member_count,
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
        self.current_poll_interval = POLL_INTERVAL_SEC

    async def on_ready(self):
        global client_ref
        client_ref = self
        logger.info(f"Selfbot ready → {self.user} | {len(self.guilds)} guilds")

        if not self.poll_task or self.poll_task.done():
            self.poll_task = asyncio.create_task(self.poll_loop())

    async def poll_loop(self):
        await asyncio.sleep(3)
        while True:
            cycle_start = time.time()
            logger.info(f"Starting poll cycle — {len(self.guilds)} guilds")

            for i, guild in enumerate(self.guilds):
                if i > 0:
                    await asyncio.sleep(GUILD_STAGGER_SEC)
                await self.poll_guild(guild)

            elapsed = time.time() - cycle_start
            wait = max(0, self.current_poll_interval - elapsed) + random.uniform(0, 5)
            logger.info(f"Cycle done in {elapsed:.1f}s | Next in {wait:.1f}s")
            await asyncio.sleep(wait)

    async def poll_guild(self, guild: discord.Guild):
        logger.info(f"Polling {guild.name} ({guild.id}) — {guild.member_count:,} members")

        members_count = 0
        try:
            if guild.member_count and guild.member_count < 5000:
                async for member in guild.fetch_members(limit=None):
                    members_count += 1
                logger.info(f"  ✅ Fetched {members_count} members via API")
            else:
                channel = next((c for c in guild.text_channels
                                if c.permissions_for(guild.me).read_messages), None)
                if channel:
                    member_ids = await scrape_members_discum(guild.id, channel.id, USER_TOKEN)
                    logger.info(f"  ✅ Scraped {len(member_ids)} members via discum")
                else:
                    logger.warning(f"  ⚠️ No readable channel in {guild.name} — using cache ({len(guild.members)} members)")
        except Exception as e:
            logger.error(f"Failed polling {guild.name}: {e}")

    async def close(self):
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
