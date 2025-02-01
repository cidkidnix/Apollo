tail -f -n +1 $1 | jq --unbuffered -c 'select(.component == "runtime_mem_stats")' | jq -r -c --unbuffered '"\(.alloc), \(.time)"' >> alloc.log
