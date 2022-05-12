cd /home/ec2-user/quark/stream/test_stream_smoke
filename=$(date +%Y%m%d)_$(date +%H%M%S)
nohup sudo python3 ../ci_runner.py --debug --local >proton-probe-$filename.log 2>&1 &