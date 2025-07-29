#!/bin/bash

set -e

PROD_SERVER=${PROD_SERVER:-"your-prod-server.com"}
PROD_USER=${PROD_USER:-"flipt"}
FLIPT_HOME=${FLIPT_HOME:-"/opt/flipt"}

echo "Deploying Flipt to production..."

# Create deployment package
echo "Creating deployment package..."
mkdir -p dist/flipt
cp bin/flipt dist/flipt/
cp deployments/prod/flipt-config.yml dist/flipt/
cp -r deployments/opa-policy dist/flipt/policy
cp scripts/flipt.service dist/flipt/
cp scripts/install-prod.sh dist/flipt/

# Create tarball
tar -czf dist/flipt-deployment.tar.gz -C dist flipt

echo "Uploading to production server..."
scp dist/flipt-deployment.tar.gz ${PROD_USER}@${PROD_SERVER}:/tmp/

echo "Installing on production server..."
ssh ${PROD_USER}@${PROD_SERVER} "
  cd /tmp && \
  tar -xzf flipt-deployment.tar.gz && \
  sudo ./flipt/install-prod.sh
"

echo "Deployment completed!"