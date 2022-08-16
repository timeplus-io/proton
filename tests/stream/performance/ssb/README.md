# SSB

##Install dependencies:
aws s3 cp s3://tp-internal/proton/proton-python-driver/proton_driver-0.2.4.tar.gz ./
aws s3 cp s3://tp-internal/proton/proton-python-driver/proton_driver-0.2.4-cp39-cp39-macosx_11_0_x86_64.whl ./

pip3 install ./proton_driver-0.2.4.tar.gz
pip3 install -r ./requirements.txt

##Usage
python3 ./ssb_run.py ./clickhouse -m all -s localhost -p 9000 -r 10
python3 ./ssb_run.py ./proton -m all -s localhost -p 8463 -r 10



