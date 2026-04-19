#!/bin/bash
# refill_credits.sh - Simulate a Stripe Subscription Renewal locally

# 1. Start the Stripe listen command in a separate terminal if you haven't:
# stripe listen --forward-to localhost:8081/webhooks/stripe

echo "🚀 Simulating Stripe Subscription Renewal ($119.99 - Team Tier)..."

# Trigger an invoice.paid event
# Replace CUSTOMER_ID with your actual Stripe Customer ID from the dashboard
# or let it create a mock one.
stripe trigger invoice.paid \
  --override invoice:amount_paid=11999 \
  --override invoice:customer=cus_test_shikipond

echo "✅ Event triggered. Check your coordinator logs to see the replenishment."
