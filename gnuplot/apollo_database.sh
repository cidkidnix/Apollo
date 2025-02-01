tail -f -n +1 $1 | jq 'select(.component == "database")' --unbuffered -c | jq --unbuffered -r -c '"\(.query_execution_time_ms), \(.time)"' >> batches.log
