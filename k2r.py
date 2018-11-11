#!/usr/local/bin/python3
import io
import random
import sys
import getopt
import json
import logging
from pprint import pformat

import redis
from rejson import Client, Path

from confluent_kafka import Producer, Consumer, KafkaException, KafkaError

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder

schema = avro.schema.Parse(open("example.avsc", "rb").read().decode())

# redis clients 
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
rejson_client = Client(host='localhost', port=6379)

# kafka setup
kafka_topic = "chit-chat"
kafka_conf = {'bootstrap.servers': '127.0.0.1'}

# kafka producer
producer = Producer(**kafka_conf)
 
for i in range(8):
    writer = avro.io.DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write({"name": "C0FFEEFACE0" + str(i), "favorite_color": "111", "favorite_number": random.randint(0,10)}, encoder)
    raw_bytes = bytes_writer.getvalue()
    producer.produce(kafka_topic, raw_bytes)
# end kafka producer

# kafka consumer
def print_assignment(consumer, partitions):
    print('Assignment:', partitions)

# group id is needed only for consumer
kafka_conf['group.id'] = 'friends_group'

c = Consumer(**kafka_conf)
c.subscribe([kafka_topic], on_assign=print_assignment)

try:
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            # Error or event
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            else:
                # Error
                raise KafkaException(msg.error())
        else:
            # great success
            sys.stderr.write('%% %s [%d] at offset %d with key %s:\n' %
                             (msg.topic(), msg.partition(), msg.offset(),
                              str(msg.key())))
            bytes_reader = io.BytesIO(msg.value())
            decoder = BinaryDecoder(bytes_reader)
            reader = DatumReader(schema)
            msg_dict = (reader.read(decoder))
            print(json.dumps(msg_dict))
            redis_client.set(msg_dict['name'], str(json.dumps(msg_dict)))
            rejson_client.jsonset(msg_dict['name'] + '_json', Path.rootPath(), msg_dict)

except KeyboardInterrupt:
    sys.stderr.write('%% Aborted by user\n')

finally:
    # Close down consumer to commit final offsets.
    c.close()
# end kafka consumer

