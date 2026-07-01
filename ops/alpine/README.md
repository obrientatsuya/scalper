# Alpine Server Recovery

Purpose: recover the headless Alpine diskless server after USB loss without
putting secrets in git.

Production shape:

- Hostname: `scalper`
- Static LAN IP: `192.168.0.205`
- SSH: key-only, root login disabled
- Firewall: SSH allowed from `192.168.0.0/24`, health endpoint local-only
- Scalper service: OpenRC service `scalper`
- Persistent app storage: `/media/usb/scalper.ext4` mounted at `/srv/scalper`
- Heartbeat: every 2 minutes to `/run/scalper/status/latest.json`; optional
  Supabase RPC push when `/etc/scalper/heartbeat.env` is configured

Secret files not tracked:

- `/etc/scalper/heartbeat.env`
- SSH private keys
- Binance API keys
- Supabase service role key
- Telegram bot token

After applying configs on Alpine:

```sh
chmod +x /usr/local/bin/scalper-heartbeat
chmod +x /etc/local.d/scalper-storage.start
chmod +x /etc/init.d/scalper
rc-update add nftables default
rc-update add crond default
rc-update add chronyd default
rc-update add sshd default
rc-update add local default
rc-update add scalper default
lbu commit -d usb
```
