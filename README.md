# Discord Member Monitor

A Python selfbot that monitors Discord servers for new members and sends notifications via webhook.

## Features

- ✅ Small servers (<5k): Full member list with accurate join timestamps
- ✅ Large servers (≥5k): Discum gateway scraping for complete member data
- ✅ Redis persistence: Never re-notifies old members
- ✅ Grace period logic: No spam on first run
- ✅ Health endpoints: `/` and `/stats` for monitoring
- ✅ Token expiry detection: Clear alerts when token dies

## Setup

### 1. Environment Variables

Create a `.env` file with:

```
USER_TOKEN=your_discord_token_here
NOTIFY_URL=https://your-webhook-bot.onrender.com/notify
UPSTASH_REDIS_REST_URL=your_upstash_url
UPSTASH_REDIS_REST_TOKEN=your_upstash_token
```

### 2. Local Testing

```bash
pip install -r requirements.txt
python main.py
```

### 3. Deploy on Render

1. Create new Web Service
2. Connect this GitHub repo
3. Set build command: `pip install -r requirements.txt`
4. Add environment variables from `.env`
5. Deploy!

## Monitoring

- **Health check**: `https://your-bot-url.onrender.com/`
- **Stats**: `https://your-bot-url.onrender.com/stats`
- **Logs**: Render dashboard → Logs

## How It Works

1. **Poll Cycle**: Runs every 10 minutes
2. **Member Fetch**:
   - Small servers: Uses discord.py-self `fetch_members()` (has join timestamps)
   - Large servers: Uses discum gateway lazy loading (no timestamps, uses poll time)
3. **Redis Comparison**: Compares fetched members against stored IDs
4. **Notification**: Posts new members to CYPHER XXD webhook
5. **Grace Period**: 5-minute window prevents old members from triggering alerts

## Architecture

- **discord.py-self**: Main bot client (single gateway connection)
- **discum**: Gateway lazy loading for large servers (separate thread)
- **Upstash Redis**: Persistent member ID storage
- **Flask**: Health + stats endpoints
- **CYPHER XXD**: Relay bot that sends Discord DMs

## Notes

- **Gateway Isolation**: Discum runs in a separate thread to avoid conflicts with main client
- **No `joined_at` from discum**: Large server members use poll time as approximation
- **First scan baseline**: All members marked as "old" to prevent initial spam
- **Rate limiting**: 2-second stagger between guild polls
