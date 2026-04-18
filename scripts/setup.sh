#!/bin/bash
set -e

REGION=${1:-iad}
WORKER_APP="duckpond-worker"
COORD_APP="duckpond-coordinator"
BUCKET_NAME="duckpond-data"

echo "======================================"
echo "🦆 Bootstrapping DuckPond in $REGION"
echo "======================================"

# 1. Create the Fly Apps (ignoring errors if they already exist)
echo ""
echo "▶ Creating Fly.io Apps..."
fly apps create $WORKER_APP || echo "App $WORKER_APP may already exist."
fly apps create $COORD_APP || echo "App $COORD_APP may already exist."

# 2. Provision Tigris S3 Bucket
# When you do this, Fly automatically injects AWS_ACCESS_KEY_ID, AWS_ENDPOINT_URL_S3, etc. into the app.
echo ""
echo "▶ Provisioning Tigris Storage Bucket ($BUCKET_NAME)..."
fly storage create --name $BUCKET_NAME --app $WORKER_APP || echo "Bucket may already exist."

# Since Tigris secrets are injected into the worker, let's extract them and inject them into the coordinator too.
# (If fly storage allows attaching the same bucket to a second app easily, we'd do that, but manual sync is safest).
echo ""
echo "▶ Syncing Tigris Secrets to Coordinator..."
echo "  Note: We need to set the same BUCKET_NAME and AWS_* keys on $COORD_APP."
# BUCKET_NAME isn't always injected by default by Tigris, so we enforce it:
fly secrets set BUCKET_NAME=$BUCKET_NAME -a $WORKER_APP
fly secrets set BUCKET_NAME=$BUCKET_NAME -a $COORD_APP

# 3. Deploy the backend services
echo ""
echo "▶ Deploying Worker Machines..."
cd worker
fly deploy -a $WORKER_APP
cd ..

echo ""
echo "▶ Deploying Coordinator Machine..."
# Optionally point the Coordinator to the Worker's private internal network
fly secrets set WORKER_FLY_HOST="${WORKER_APP}.internal" WORKER_COUNT=4 -a $COORD_APP
cd coordinator
fly deploy -a $COORD_APP
cd ..

echo ""
echo "======================================"
echo "✅ DuckPond Deployment Complete!"
echo "======================================"
echo ""
echo "Your Coordinator URL is: https://${COORD_APP}.fly.dev"
echo ""
echo "To finish setup in the UI, grab your Tigris access keys using:"
echo "  fly secrets list -a $WORKER_APP"
echo ""
