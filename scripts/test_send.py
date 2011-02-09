#!/usr/bin/env python

import amqplib.client_0_8 as amqp

def main():
	conn = amqp.Connection("localhost")
	ch = conn.channel()
	ch.exchange_declare('test', 'topic', auto_delete=False)
	ch.queue_declare('test', auto_delete=False)
	ch.queue_bind('test', 'test')

	msg = amqp.Message('{"test":"value", "int": 14}', 
		content_type='application/json', 
		application_headers={'X-Riak-Meta-Foo': 7, 'X-Riak-Meta-Bar': 'baz'},
		delivery_mode=2
	)

	ch.basic_publish(msg, 'test')

	ch.close()
	conn.close()

if __name__ == '__main__':
    main()

