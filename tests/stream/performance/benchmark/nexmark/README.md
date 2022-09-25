# Nexmark

load nexmark data into proton:

python ./fload.py -s person -d ./data/person_ddl.sql -b 10000 ./data/person.csv
python ./fload.py -s auction -d ./data/auction_ddl.sql -b 10000 ./data/auction.csv
python ./fload.py -s bid -d ./data/bid_ddl.sql -b 10000 ./data/bid.csv



