import asyncio
import discord
from discord import Intents
import discum
import time
import threading
import os

USER_TOKEN = os.getenv("USER_TOKEN")

if not USER_TOKEN:
    print("❌ USER_TOKEN environment variable not set")
    exit(1)

POLL_INTERVAL_SEC = 120
DISCUM_TIMEOUT_SEC = 25

async def scrape_members_discum(guild_id, channel_id, token):
    member_ids = set()
    stop_event = threading.Event()
    last_size = 0

    def run_discum():
        bot = discum.Client(token=token, log=False)
        @bot.gateway.command
        def on_ready(resp):
            if resp.event.ready_supplemental:
                print(f"[DISCUM] Fetching members for guild {guild_id}")
                bot.gateway.fetchMembers(guild_id, channel_id, reset=True)
        @bot.gateway.command
        def on_chunk(resp):
            if resp.event.guild_members_chunk:
                for member in resp.parsed.auto():
                    try:
                        member_ids.add(str(member['user']['id']))
                    except: pass
                print(f"[DISCUM] Chunk → total {len(member_ids)} members")
        bot.gateway.run(auto_reconnect=False)
        stop_event.set()

    thread = threading.Thread(target=run_discum, daemon=True)
    thread.start()
    start = time.time()
    while time.time() - start < DISCUM_TIMEOUT_SEC:
        await asyncio.sleep(0.5)
        if len(member_ids) == last_size and len(member_ids) > 100:
            break
        last_size = len(member_ids)
    stop_event.set()
    print(f"[DISCUM] Final: {len(member_ids)} members for guild {guild_id}")
    return member_ids

class LogOnlyMonitor(discord.Client):
    async def on_ready(self):
        print(f"✅ Logged in as {self.user} | Monitoring {len(self.guilds)} guilds")
        self.loop.create_task(self.poll_loop())

    async def poll_loop(self):
        await asyncio.sleep(5)
        while True:
            print(f"\n🔄 Poll cycle start – {len(self.guilds)} guilds")
            for guild in self.guilds:
                await self.poll_guild(guild)
            await asyncio.sleep(POLL_INTERVAL_SEC)

    async def poll_guild(self, guild):
        print(f"\n🔍 Polling {guild.name} (ID {guild.id}) – {guild.member_count} members")
        if guild.member_count and guild.member_count < 5000:
            count = 0
            async for member in guild.fetch_members(limit=None):
                count += 1
                if count <= 3:
                    print(f"  Example: {member}")
            print(f"  ✅ Fetched {count} members via API")
        else:
            channel = next((c for c in guild.text_channels if c.permissions_for(guild.me).read_messages), None)
            if channel:
                member_ids = await scrape_members_discum(guild.id, channel.id, USER_TOKEN)
                print(f"  ✅ Scraped {len(member_ids)} members via discum")
            else:
                print(f"  ⚠️ No readable channel – using cache ({len(guild.members)} members)")

if __name__ == "__main__":
    intents = Intents.default()
    intents.members = True
    client = LogOnlyMonitor(intents=intents)
    client.run(USER_TOKEN, bot=False)
