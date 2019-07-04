#!/usr/bin/env python
"""how to run python log_processor.py 'web.*' 
* can represent one word
# can represent zero or more words

the idea is to have many  handlers receive specify messages or task
"""
import sys
import time
import random

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# create an exchange for for the queue to listen to 
exchange_name = 'logs'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

# don't specifiy a queue name to get a random queue
# since this is a random queue, we would like to delete it when the processer is
# done; so we pass exclusive
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

severities = sys.argv[1:]
if not severities:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

# bind the queue to the specified routing key, sender would be sent to an exchange with route key
# queue listen to message with a route key 
for severity in severities:
    channel.queue_bind(
        exchange=exchange_name, queue=queue_name, routing_key=severity)

print(' [*] Waiting for logs. To exit press CTRL+C')


def process_message(ch, method, properties, body):
    print(" [x] Received %r via %s" % (body, method.routing_key))
    # let's pretend that we are doing some serious task
    time.sleep(random.randint(1,10))
    print(" [x] Done")
    # sent a flag that the message has been processes successfully
    ch.basic_ack(delivery_tag = method.delivery_tag) 


channel.basic_consume(queue=queue_name, on_message_callback=process_message)
channel.start_consuming()