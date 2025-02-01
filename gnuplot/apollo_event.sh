cat $1 | jq -c 'select(.component == "parsed_data")' | jq -r -c '"\(.creates_size), \(.time)"' > creates_data_filtered.txt
cat $1 | jq -c 'select(.component == "parsed_data")' | jq -r -c '"\(.exercises_size), \(.time)"' > exercises_data_filtered.txt
