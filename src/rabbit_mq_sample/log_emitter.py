#!/usr/bin/env python
"""how to run python log_emitter.py warning error > 'sender is down' """
import sys
import time
import pika

from faker import Faker


fake = Faker()

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

"""
we create an exchange that would recieve and publish messages to queues
we shall not use a queue since queues are now bind to an exchange

The strategy is to broadcast message and describe the route that is shall be
sent to via a pattern. Processes listen to the exchange that match any of them
queue topic pattern
"""

# create an exchange that handler the traffic
exchange_name = 'logs'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

severity = sys.argv[1] if len(sys.argv) > 1 else 'info'

# let's simulate many people sending task to our server
sending_interval = int(sys.argv[2]) if len(sys.argv) > 2 else 10
while True:
    message = fake.name()
    channel.basic_publish(exchange=exchange_name,
                      routing_key=severity,
                      body=message,
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent, rabbitmq would keep the message even when it crushes
                      ))
    print('sent {} with topic {}'.format(message, severity))
    print('sleep for %d second' % sending_interval)
    time.sleep(sending_interval)

print(' [*] Waiting for messages. To exit press CTRL+C')
connection.close()