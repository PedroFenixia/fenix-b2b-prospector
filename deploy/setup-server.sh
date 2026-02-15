#!/bin/bash
# ============================================
# FENIX B2B Prospector - Server Setup Script
# Run this on your OVH VPS as root
# ============================================
set -e

echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║  FENIX B2B Prospector - Server Setup ║"
echo "  ╚══════════════════════════════════════╝"
echo ""

# 1. System updates + dependencies
echo "[1/7] Installing system dependencies..."
apt update && apt upgrade -y
apt install -y python3 python3-venv python3-pip git nginx certbot python3-certbot-nginx ufw

# 2. Create user
echo "[2/7] Creating fenix user..."
if ! id "fenix" &>/dev/null; then
    useradd -m -s /bin/bash fenix
fi

# 3. Clone repo
echo "[3/7] Cloning repository..."
if [ -d "/opt/fenix-b2b" ]; then
    cd /opt/fenix-b2b && git pull origin main
else
    git clone https://github.com/PedroFenixia/fenix-b2b-prospector.git /opt/fenix-b2b
fi
chown -R fenix:fenix /opt/fenix-b2b

# 4. Python environment
echo "[4/7] Setting up Python environment..."
cd /opt/fenix-b2b
sudo -u fenix python3 -m venv .venv
sudo -u fenix .venv/bin/pip install --upgrade pip
sudo -u fenix .venv/bin/pip install -e ".[dev]" 2>/dev/null || sudo -u fenix .venv/bin/pip install -e .

# 5. Systemd service
echo "[5/7] Installing systemd service..."
cp deploy/fenix-b2b.service /etc/systemd/system/fenix-b2b.service
systemctl daemon-reload
systemctl enable fenix-b2b
systemctl start fenix-b2b

# Wait for app to start
sleep 3
echo "  App status: $(systemctl is-active fenix-b2b)"

# 6. Nginx
echo "[6/7] Configuring Nginx..."
# First, set up HTTP-only config for certbot
cat > /etc/nginx/sites-available/fenix-b2b << 'NGINX'
server {
    listen 80;
    server_name b2b.fenixia.tech;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /static/ {
        alias /opt/fenix-b2b/app/web/static/;
        expires 7d;
    }
}
NGINX

ln -sf /etc/nginx/sites-available/fenix-b2b /etc/nginx/sites-enabled/
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl reload nginx

# 7. Firewall
echo "[7/7] Configuring firewall..."
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw --force enable

echo ""
echo "  ✓ Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Point DNS: b2b.fenixia.tech -> $(curl -s ifconfig.me)"
echo "  2. After DNS propagates, run:"
echo "     certbot --nginx -d b2b.fenixia.tech"
echo "  3. Visit: https://b2b.fenixia.tech"
echo ""
echo "  Useful commands:"
echo "    systemctl status fenix-b2b    # Check app status"
echo "    journalctl -u fenix-b2b -f    # View logs"
echo "    systemctl restart fenix-b2b   # Restart app"
echo ""
