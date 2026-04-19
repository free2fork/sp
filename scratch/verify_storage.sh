#!/bin/bash
# verify_storage.sh - Verify that the real-time storage bar is receiving data

COORDINATOR_URL="http://localhost:8081"
# You may need to provide a valid JWT token here if running against a protected coordinator
# TOKEN="YOUR_JWT_HERE"

echo "📊 Checking initial storage usage..."
curl -s -H "Authorization: Bearer $TOKEN" "$COORDINATOR_URL/usage" | json_pp

echo -e "\n📤 Creating and uploading a 5MB test file..."
dd if=/dev/urandom of=test_data.parquet bs=1M count=5 2>/dev/null

curl -s -X POST -H "Authorization: Bearer $TOKEN" \
  -F "file=@test_data.parquet" \
  -F "table_name=storage_verification_test" \
  "$COORDINATOR_URL/upload" | json_pp

echo -e "\n📈 Verifying usage increase..."
curl -s -H "Authorization: Bearer $TOKEN" "$COORDINATOR_URL/usage" | json_pp

rm test_data.parquet
echo -e "\n✅ Done. If the numbers increased, your sidebar bar is now live!"
