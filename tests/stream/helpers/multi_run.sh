run_times=`expr $2 + 0`
echo "running command = '$1' for $run_times times"
i=0
while (($i <= $run_times))
do
    echo "running $i time(s)"
    #pytest -s -v ./tests/test_stream.py
    $1
    let "i++"
done