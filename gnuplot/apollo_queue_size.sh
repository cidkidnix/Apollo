tail -f -n +1 $1 | jq -c --unbuffered 'select(.component == "transaction_queue")' | jq --unbuffered -r -c '"\(.queue_size), \(.time)"' >> queue_size_filtered.log
