import os, sys, logging, time, datetime, socket
from confluent_kafka import Producer, Consumer
from argparse import ArgumentParser, FileType
from configparser import ConfigParser


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))    



if __name__ == "__main__":

    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    




    conf = {
        'bootstrap.servers': 'kafka:29092',
        'client.id': socket.gethostname()
    }

    producer = Producer(conf)
    for i in range(10):
        producer.produce('test', key=f"{i}", value="value", callback=acked)
    print("produce done.")
    producer.poll(1)