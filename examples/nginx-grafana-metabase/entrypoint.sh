#!/bin/bash

# pause otherwise it fails with "Code: 210. DB::NetException: Connection refused (proton:8463). (NETWORK_ERROR)"
sleep 10 && proton client --host $PROTON_HOST --multiquery < /tmp/01_nginx_access_log.sql

DEST=/tmp
export_csv=nginx_export.csv
import_csv=nginx_import.csv
export_ipinfo=nginx_export.ipinfo
import_ipinfo=nginx_import.ipinfo.csv

# export the random stream to a CSV
proton client --host $PROTON_HOST --multiquery < /tmp/02_csv-export.sql > $DEST/$export_csv

echo "Using the following config for IPinfo:"
cat ~/.config/ipinfo/config.json

# extract IP addresses from the exported .csv file
echo "Extracting only IP addresses from $DEST/$export_csv to $DEST/$export_ipinfo.tmp ..."
cat "$DEST"/$export_csv | cut -d ',' -f 1 | sed 's/^"//; s/"$//' > "$DEST"/"$export_ipinfo.tmp"
echo "Removing duplicate IP addresses from $DEST/$export_ipinfo.tmp ..."
sort "$DEST"/"$export_ipinfo.tmp" | uniq > "$DEST"/"$export_ipinfo"
echo "Combined all IP addresses into a single file: $DEST/$export_ipinfo."
rm -v "$DEST"/"$export_ipinfo.tmp"

echo "Extracting the rest of the data from $DEST/$export_csv to $DEST/$import_csv ..."
cat "$DEST"/$export_csv | cut -d ',' -f 2- > "$DEST"/"$import_csv"


# lookup all the IP addresses using ipinfo.io
echo "Geo-locating all the IP addresses in bulk using the IPInfo API (https://ipinfo.io):"
ip_count=$(wc "$DEST"/$export_ipinfo | awk '{print $1}')
echo "   Total IP addresses that will be looked up in bulk using $DEST/$export_ipinfo: $ip_count." 
cat "$DEST"/$export_ipinfo | ipinfo -c > "$DEST"/"$import_ipinfo"
echo "   Geo-lookup of $ip_count IP addresses written to file: $DEST/$import_ipinfo."
echo "   Complete!"


#proton client --host $PROTON_HOST --multiquery < /tmp/03_csv-import.sql

query01=`cat /tmp/03_csv-import-01.sql`
proton client --host $PROTON_HOST -q "$query01" < /tmp/nginx_import.csv

query02=`cat /tmp/03_csv-import-02.sql`
proton client --host $PROTON_HOST --multiquery -q "$query02" < /tmp/nginx_import.ipinfo.csv
