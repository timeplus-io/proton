# Demo for real-time machine learning - fraud detection

This docker compose file demonstrates how to leverage proton to build a real-time fraud detection where proton is used as a real-time feature store.

## Start the example

Simply run `docker compose up` in this folder. three docker containers in the stack:
1. d.timeplus.com/timeplus-io/proton:latest, as the streaming database.
2. timeplus/fraud:latest, a online payment transaction data generator
3. jupyter/scipy-notebook:latest, jupyter notebook


## Run Notebook

Visit `http://localhost:8888/notebooks/work/fraud_detection.ipynb` to access the notebook. And then just follow the code in the notebook step by step.
