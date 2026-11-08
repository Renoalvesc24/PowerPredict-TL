#!/usr/bin/env bash
# Runs ON the Oracle Cloud server (Ubuntu 24.04, ARM). Installs Docker, opens ports 80/443,
# adds swap, unpacks the bundle into /opt/powerpredict, creates random database passwords
# and starts the whole system.
#
#   bash remote_setup.sh <domain>        e.g. bash remote_setup.sh powerpredict-tl.duckdns.org
set -euo pipefail
APP=/opt/powerpredict
DOMAIN="${1:?usage: remote_setup.sh <domain>}"

echo "== 1/5 packages (Docker)"
sudo apt-get update -y -q
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q docker.io docker-compose-v2 iptables-persistent netfilter-persistent openssl
sudo systemctl enable --now docker

echo "== 2/5 firewall: allow 80/443 (Oracle Ubuntu images reject everything except SSH)"
for p in 80 443; do
  if ! sudo iptables -C INPUT -p tcp --dport "$p" -m state --state NEW -j ACCEPT 2>/dev/null; then
    pos=$(sudo iptables -L INPUT --line-numbers | awk '/REJECT/ {print $1; exit}')
    if [ -n "$pos" ]; then sudo iptables -I INPUT "$pos" -p tcp --dport "$p" -m state --state NEW -j ACCEPT
    else sudo iptables -A INPUT -p tcp --dport "$p" -m state --state NEW -j ACCEPT; fi
  fi
done
sudo netfilter-persistent save

echo "== 3/5 swap (4 GB safety margin for Spark)"
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 4G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile >/dev/null && sudo swapon /swapfile
  grep -q '/swapfile' /etc/fstab || echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab >/dev/null
fi

echo "== 4/5 unpack"
sudo mkdir -p "$APP"
sudo tar -xzf /tmp/powerpredict_bundle.tar.gz -C "$APP"
sudo chown -R "$USER":"$USER" "$APP"
cd "$APP/deploy"
if [ ! -f server.env ]; then
  ( umask 077
    printf 'DB_PASSWORD=%s\nDB_ROOT_PASSWORD=%s\nPP_DOMAIN=%s\n' "$(openssl rand -hex 24)" "$(openssl rand -hex 24)" "$DOMAIN" > server.env )
else
  sed -i "s|^PP_DOMAIN=.*|PP_DOMAIN=$DOMAIN|" server.env
fi

echo "== 5/5 build and start (first build takes a few minutes)"
sudo docker compose --env-file server.env up -d --build
sudo docker compose --env-file server.env ps
