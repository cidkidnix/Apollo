tail -f -n +1 $1 | jq 'select(.component == "database_pool")' --unbuffered -c | jq --unbuffered -r -c '"\(.acquired_connections), \(.idle_connections), \(.time)"' >> connection_pool.log
