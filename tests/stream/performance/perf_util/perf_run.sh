#usage: ./perf_run.sh <chameleon generator path> <proton version>
#e.g. ./perf_run.sh ~/chameleon/generator 1.0.38
echo "chameleon generator path = $1"
if [ ! -n "$1" ]
then
  echo "no chameleon path name input, exit"
  echo "usage: perf_run chameleon_generator_path_name, note: the path name should not end with /"
  exit
fi
proton_version=$(docker exec -i proton-server proton-client --query="select version()")
if [ $? -gt 0 ]
then 
  echo "getting proton_version error, please check if proton-server container running well, exit."
  exit
fi  
echo "proton version: $proton_version"

cp ./neutron_race_middle.json $1/samples/json/
cp ./neutron_race_large.json $1/samples/json/
cp ./neutron_race_xlarge.json $1/samples/json/
cd $1
for file in `ls $proton_version_neutron_*.csv`
do
  echo "$file to be removed."
  sudo rm -rf "$file"
done
ls -l
wait
go run main.go -f ./samples/json/neutron_race_middle.json
wait
c=0
for file in `ls timeplus_*report*.csv`
do
  echo "$file is generated"
  timestamp=$(date +%s)
  mv "$file" $proton_version"_neutron_latency_middle_"$timestamp.csv
  wait
  aws s3 cp ./$proton_version"_neutron_latency_middle_"$timestamp.csv s3://tp-internal/proton/perf/$proton_version"_neutron_latency_middle_"$timestamp.csv
  wait
  echo "$file is renamed to $proton_version"_neutron_latency_middle_"$timestamp.csv and uploaded to s3..." 
  ((C++))
done
wait
go run main.go -f ./samples/json/neutron_race_large.json
wait
c=0
for file1 in `ls timeplus_*report*.csv`
do
  echo "$file1 is generated and rename and upload..."
  timestamp=$(date +%s)
  mv "$file1" $proton_version"_neutron_latency_large_"$timestamp.csv
  wait
  aws s3 cp ./$proton_version"_neutron_latency_large_"$timestamp.csv s3://tp-internal/proton/perf/$proton_version"_neutron_latency_large_"$timestamp.csv  
  wait
  echo "$file1 is renamed to $proton_version"_neutron_latency_large_"$timestamp.csv and uploaded to s3..." 
  ((C++))
done
wait
go run main.go -f ./samples/json/neutron_race_xlarge.json
wait
c=0
for file2 in `ls timeplus_*report*.csv`
do
  echo "$file2 is generated and rename and upload..."
  timestamp=$(date +%s)
  mv "$file2" $proton_version"_neutron_latency_xlarge_"$timestamp.csv
  wait
  aws s3 cp ./$proton_version"_neutron_latency_xlarge_"$timestamp.csv s3://tp-internal/proton/perf/$proton_version"_neutron_latency_xlarge_"$timestamp.csv  
  wait
  echo "$file2 is renamed to $proton_version"_neutron_latency_xlarge_"$timestamp.csv and uploaded to s3..." 
  ((C++))
done
wait
