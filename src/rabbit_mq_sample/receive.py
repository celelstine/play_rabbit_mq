#!/usr/bin/env python
import time
import pika

# create a connection on a host and create a channel for connection
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# create or get a queue 
queue_name = 'family1'
# ensure that this queue is persistent even after a shut down
channel.queue_declare(queue=queue_name, durable=True)


# function to process incoming message
def process_message(ch, method, properties, body):
    print(" [x] Received %r via %s" % (body, method.routing_key))
    # let's pretend that we are doing some serious task
    time.sleep(len(body))
    print(" [x] Done")
    # sent a flag that the message has been processes successfully
    ch.basic_ack(delivery_tag = method.delivery_tag) 


# declare that we want to be notified when a message is sent to this queue
channel.basic_consume(queue=queue_name,
                      auto_ack=False, # we want to ack manually
                      on_message_callback=process_message)

# ensure that a worker does not receive more than one message at a time
# so workers would recieve new message only when they acknowledge an old message
channel.basic_qos(prefetch_count=1)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()