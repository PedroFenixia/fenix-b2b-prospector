#!/bin/bash
# Quick update script - run on VPS after pushing changes
set -e
cd /opt/fenix-b2b
git pull origin main
sudo -u fenix .venv/bin/pip install -e . --quiet
systemctl restart fenix-b2b
echo "Updated and restarted. Status: $(systemctl is-active fenix-b2b)"
