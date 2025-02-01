cat $1 | jq 'select(.component == "database")' -c | jq -r -c '"\(.query_execution_time_ms), \(.time)"' > batches.log
