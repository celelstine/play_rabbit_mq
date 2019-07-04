#!/usr/bin/env python
import sys
import time

import pika
from faker import Faker


fake = Faker()

# create a connection on a host and create a channel for connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# create or get a queue and send a message to that queue
queue_name = 'family1'
# ensure that this queue is persistent even after a shut down
channel.queue_declare(queue=queue_name, durable=True)

sending_interval = int(sys.argv[1]) if len(sys.argv) > 1 else 10
# let's simulate many people sending task to our server
while True:
    message = fake.name()
    channel.basic_publish(exchange='', # we are not using an exhange yet
                      routing_key=queue_name, # this is the name of the queue
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent, rabbitmq would keep the message even when it crushes
                      ))
    print('sent {} to queue {}'.format(message, queue_name))
    print('sleep for %d second' % sending_interval)
    time.sleep(sending_interval)

print(' [*] Waiting for messages. To exit press CTRL+C')
connection.close()
