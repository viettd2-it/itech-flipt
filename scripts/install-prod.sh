#!/bin/bash

set -e

FLIPT_HOME="/opt/flipt"
FLIPT_USER="flipt"
FLIPT_GROUP="flipt"

echo "Installing Flipt on production server..."

# Create user and directories
if ! id "$FLIPT_USER" &>/dev/null; then
    echo "Creating flipt user..."
    useradd -r -s /bin/false -d $FLIPT_HOME $FLIPT_USER
fi

# Create directories
mkdir -p $FLIPT_HOME/{bin,config,policy,logs}
mkdir -p /var/log/flipt
mkdir -p /etc/systemd/system

# Copy files
echo "Copying files..."
cp flipt $FLIPT_HOME/bin/
cp flipt-config.yml $FLIPT_HOME/config/
cp -r policy/* $FLIPT_HOME/policy/
cp flipt.service /etc/systemd/system/

# Set permissions
chown -R $FLIPT_USER:$FLIPT_GROUP $FLIPT_HOME
chown -R $FLIPT_USER:$FLIPT_GROUP /var/log/flipt
chmod +x $FLIPT_HOME/bin/flipt

# Reload systemd and enable service
systemctl daemon-reload
systemctl enable flipt
systemctl restart flipt

echo "Flipt installed and started successfully!"
echo "Check status with: systemctl status flipt"
echo "View logs with: journalctl -u flipt -f"