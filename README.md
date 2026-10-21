# IVAO WebEye track recorder

Saves every IVAO pilot's position once a minute, so the map can draw any flight's **full track from pushback** for every visitor, with no API key and no tab left open. It reads only IVAO's public Whazzup feed, the same one the map uses.

Runs free on Cloudflare Workers (one SQLite Durable Object + a once-a-minute cron). It keeps the last 18 hours and stays well inside the free plan: about 47,000 of 100,000 daily row writes.

## Set it up (about 5 minutes, free)

1. Click **[Deploy to Cloudflare](https://deploy.workers.cloudflare.com/?url=https://github.com/etihadairways1028-beep/IVAO-Webeye-2.0/tree/main/recorder)** and sign in (or create a free account).
2. Keep the name `webeye-recorder` and press **Create and deploy**.
3. When it's done, open the worker link it shows (like `https://webeye-recorder.<your-name>.workers.dev`). You should see `"service": "IVAO WebEye track recorder"`. After a minute or two `pilots` shows how many pilots it saved.
4. Put that link in the site: GitHub repo → **Settings → Secrets and variables → Actions → Variables → New repository variable**, name `VITE_RECORDER_URL`, value the link. Then re-run the **Deploy to GitHub Pages** action (or push any change). On Cloudflare Pages, add the same variable under **Settings → Variables and secrets**.

Tracks start from the moment the recorder is running: flights already airborne then get their track from that point on.

## API

- `GET /tracks/<sessionId>`: positions of one IVAO session, oldest first (`latitude, longitude, altitude, groundSpeed, onGround, timestamp`).
- `GET /tracks/<sessionId>?since=<ms>`: only points after that time.
- `GET /status`: last recording time and pilot count.

## Local test

```
npm install
npx wrangler dev --test-scheduled
curl "localhost:8787/__scheduled?cron=*+*+*+*+*"
curl localhost:8787/status
```
