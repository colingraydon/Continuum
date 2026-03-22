#!/bin/bash

URL=${1:-"http://localhost:8080"}
NUM_KEYS=${2:-100}

echo "Posting $NUM_KEYS keys to $URL..."

for i in $(seq 1 $NUM_KEYS); do
    KEY="key-$i"
    RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" "$URL/keys/$KEY")
    if [ "$RESPONSE" != "200" ]; then
        echo "WARNING: key $KEY returned $RESPONSE"
    fi
done

echo "Done. Fetching stats..."
curl -s "$URL/stats" | python3 -m json.tool