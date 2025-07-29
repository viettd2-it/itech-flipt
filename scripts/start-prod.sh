#!/bin/bash

# Simple script to start Flipt in production
cd /opt/flipt
./bin/flipt --config ./config/flipt-config.yml --force-migrate