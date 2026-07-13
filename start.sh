#!/bin/bash
set -e
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"
echo "Restarting dump-sniper via systemctl..."
systemctl enable dump-sniper.service 2>/dev/null || true
systemctl restart dump-sniper.service
sleep 3
systemctl is-active --quiet dump-sniper.service \
  && { echo "Started OK"; systemctl status dump-sniper.service --no-pager | head -5; } \
  || { echo "ERROR: not running"; systemctl status dump-sniper.service --no-pager; exit 1; }
