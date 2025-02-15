#!/usr/bin/env bash

# Define the source and destination directories
src_dir="_cfg_byo_lv_old"
dst_dir="_cfg_byo_lv"

declare -A months
months=( ["Jan"]="01" ["Feb"]="02" ["Mar"]="03" ["Apr"]="04" ["May"]="05" ["Jun"]="06" ["Jul"]="07" ["Aug"]="08" ["Sep"]="09" ["Oct"]="10" ["Nov"]="11" ["Dec"]="12" )

# Loop through matching files
for file in "$src_dir"/*.zip; do
  month=$(echo "$file" | awk -F'_' '{print $8}' | xargs)
  year=$(echo "$file" | awk -F'_' '{print $9}' | cut -d'.' -f1)
  new_filename="$dst_dir/LV_${year}${months[${month}]}01.zip"

  if [[ ! -f "${new_filename}" ]]; then
    echo cp "$file" "$new_filename"
    echo "Renamed: $file -> $new_filename"
  else
    echo "Diffing $file with $new_filename"
    diff "$file" "$new_filename"
  fi
done
