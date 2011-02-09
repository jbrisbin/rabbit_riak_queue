#!/usr/bin/env python

import amqplib.client_0_8 as amqp

def on_msg(msg):
	print msg.body
	#msg.channel.basic_ack(msg.delivery_tag)
	#msg.channel.basic_cancel(msg.consumer_tag)
	
def main():
	conn = amqp.Connection("localhost")
	ch = conn.channel()
	ch.exchange_declare('test', 'topic', auto_delete=False)
	ch.queue_declare('test', auto_delete=False)
	ch.queue_bind('test', 'test')

	ch.basic_consume('test', callback=on_msg)
	while ch.callbacks:
		ch.wait()

	ch.close()
	conn.close()

if __name__ == '__main__':
    main()

