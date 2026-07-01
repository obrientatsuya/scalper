#!/bin/sh
set -eu

ROOT_DIR="${1:-/mnt/repo/ops/alpine}"

install -Dm600 "$ROOT_DIR/etc/nftables.nft" /etc/nftables.nft
install -Dm755 "$ROOT_DIR/etc/init.d/scalper" /etc/init.d/scalper
install -Dm755 "$ROOT_DIR/etc/local.d/scalper-storage.start" /etc/local.d/scalper-storage.start
install -Dm644 "$ROOT_DIR/etc/network/interfaces" /etc/network/interfaces
install -Dm600 "$ROOT_DIR/etc/crontabs/root" /etc/crontabs/root
install -Dm755 "$ROOT_DIR/usr/local/bin/scalper-heartbeat" /usr/local/bin/scalper-heartbeat
install -Dm644 "$ROOT_DIR/srv/scalper/config/scalper.toml" /srv/scalper/config/scalper.toml

mkdir -p /etc/scalper
if [ ! -f /etc/scalper/heartbeat.env ]; then
  install -Dm600 "$ROOT_DIR/etc/scalper/heartbeat.env.example" /etc/scalper/heartbeat.env
fi

while read -r line; do
  grep -q "^$line" /etc/ssh/sshd_config || printf '%s\n' "$line" >> /etc/ssh/sshd_config
done < "$ROOT_DIR/etc/ssh/sshd_config.hardening"

rc-update add nftables default 2>/dev/null || true
rc-update add crond default 2>/dev/null || true
rc-update add chronyd default 2>/dev/null || true
rc-update add sshd default 2>/dev/null || true
rc-update add local default 2>/dev/null || true
rc-update add scalper default 2>/dev/null || true

lbu commit -d usb
