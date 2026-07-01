# Observability: Supabase + Telegram

Goal: server sends compact status every 2 minutes; Supabase stores only current
state; Supabase monitor sends Telegram only on state changes or critical alerts.

## Supabase Tables

Run migration:

```sh
supabase db push
```

Or paste `supabase/migrations/202607010001_server_monitor.sql` in Supabase SQL
Editor.

Create a long ingest token locally, then set its hash in Supabase:

```sql
select public.set_server_heartbeat_token(
  'scalper-alpine',
  'REPLACE_WITH_LONG_RANDOM_TOKEN'
);
```

## Server Secrets

On Alpine, edit `/etc/scalper/heartbeat.env`:

```sh
SUPABASE_URL=https://PROJECT_REF.supabase.co
SUPABASE_ANON_KEY=REPLACE_WITH_ANON_KEY
HEARTBEAT_ID=scalper-alpine
HEARTBEAT_INGEST_TOKEN=REPLACE_WITH_LONG_RANDOM_TOKEN
```

Then persist diskless config:

```sh
lbu commit -d usb
```

The server must not store Supabase service role key.

## Telegram Secrets

Create Telegram bot with BotFather and obtain `TELEGRAM_BOT_TOKEN`.
Get `TELEGRAM_CHAT_ID` from Telegram API or a helper bot.

Set Edge Function secrets:

```sh
supabase secrets set SUPABASE_URL=https://PROJECT_REF.supabase.co
supabase secrets set SUPABASE_SERVICE_ROLE_KEY=REPLACE_WITH_SERVICE_ROLE_KEY
supabase secrets set TELEGRAM_BOT_TOKEN=REPLACE_WITH_TELEGRAM_BOT_TOKEN
supabase secrets set TELEGRAM_CHAT_ID=REPLACE_WITH_TELEGRAM_CHAT_ID
supabase secrets set HEARTBEAT_ID=scalper-alpine
supabase secrets set MONITOR_SECRET=REPLACE_WITH_LONG_RANDOM_MONITOR_SECRET
```

Optional thresholds:

```sh
supabase secrets set HEARTBEAT_STALE_SECONDS=300
supabase secrets set RAM_CRITICAL_PERCENT=85
supabase secrets set DISK_CRITICAL_PERCENT=90
supabase secrets set TEMP_CRITICAL_C=80
supabase secrets set NO_EVENTS_SECONDS=300
```

## Deploy Monitor

```sh
supabase functions deploy monitor-heartbeat
```

Run `supabase/sql/schedule_monitor_heartbeat.sql` in SQL Editor after replacing:

- `PROJECT_REF`
- `SUPABASE_ANON_KEY`
- `MONITOR_SECRET`

## Behavior

- Healthy heartbeat: table row `server_heartbeat.id = scalper-alpine` updates.
  Normal reads require an authenticated Supabase user or service role. Do not
  expose server/PnL status through an anonymous public page.
- Server power/network loss: heartbeat becomes stale; Supabase monitor sends
  Telegram after `HEARTBEAT_STALE_SECONDS`.
- Scalper failure while Linux is alive: heartbeat still updates, but monitor
  sends degraded/down alert based on service/HTTP/health fields.
- Recovery: monitor sends one recovery message when state returns to healthy.
- No periodic Telegram spam.
