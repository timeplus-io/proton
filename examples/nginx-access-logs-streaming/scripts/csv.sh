#!/bin/bash

# process the log files exported through NFS from the EC2 server hosting the Ghost blog
SRC=/mnt/nginx

# $SRC is a readonly NFS share which is why we are creating a writable folder under /mnt
DEST=/mnt/csv/nginx

sudo mkdir -p $DEST
sudo chown -R ubuntu:ubuntu $DEST


echo "Deleting old csv files."
rm -v "$DEST"/*.csv

# copy and rename access.log to access.log.0 (ignore error.log)
cp -v "$SRC"/access.log "$DEST"/access.log.0

# copy the rest of the rotated access.log.* files
cp -v "$SRC"/access.log.* $DEST


# decompress all gzipped files in the destination
for file in $(ls "$DEST"/*.gz); do
    # Check if the file exists and has a .gz extension
    if [[ -f "$file" && "$file" == *.gz ]]; then
        echo "Decompressing gzipped $file."
        gunzip $file
    fi
done

# convert all access.log* files to *.csv files
for log in $(ls "$DEST"/access.log*); do
	python3 accesslog2csv.py $log "$log.csv"
	rm -v $log
done


log_csv=access.log.csv


# remove the CSV headers in log files 1..14
for csv in $(ls "$DEST"/access.log.{1..14}.csv); do
	echo "Removing CSV headers from $csv."
	tail -n +2 $csv > "$csv.1"
	mv "$csv.1" $csv
done

# combine all access.log*csv files into a single access.log.csv file
cat "$DEST"/access.log.{0..14}.csv > "$DEST"/$log_csv
echo "Combined all access logs into a single file: $DEST/$log_csv."
rm -v "$DEST"/access.log.{0..14}.csv


# extract IP addresses from each *.csv file, then remove duplicates
ipinfo="access.ipinfo"
echo "Extracting only IP addresses from $DEST/$log_csv to $DEST/$ipinfo.tmp ..."
tail -n +2 "$DEST"/$log_csv | cut -d ',' -f 1 > "$DEST"/"$ipinfo.tmp"
echo "Removing duplicate IP addresses from $DEST/$ipinfo.tmp ..."
sort "$DEST"/"$ipinfo.tmp" | uniq > "$DEST"/"$ipinfo"
echo "Combined all IP addresses into a single file: $DEST/$ipinfo."
rm -v "$DEST"/"$ipinfo.tmp"


# lookup all the IP addresses using ipinfo.io
echo "Geo-locating all the IP addresses in bulk using the IPInfo API (https://ipinfo.io):"
ip_count=$(wc "$DEST"/$ipinfo | awk '{print $1}')
echo "   Total IP addresses that will be looked up in bulk using $DEST/$ipinfo: $ip_count." 
cat "$DEST"/$ipinfo | ipinfo -c > "$DEST"/"$ipinfo.csv"
echo "   Geo-lookup of $ip_count IP addresses written to file: $DEST/$ipinfo.csv."
echo "   Complete!"
#rm -v "$DEST"/$ipinfo
