#!/usr/bin/env python

# Copyright (c) 2017 Dojo Madness GmbH

# This script is intended to help speed up testing. It does not provide full coverage
# and should not be treated as an acceptance test.

import json
import random
import string
import time

import click
from faker import Faker
import pika


def generate_routing_key():
    '''generate some routing key'''
    _id = ''.join([random.choice(string.digits) for _ in range(16)])
    return 'coll{:02d}.{:s}'.format(random.randint(0, 9), _id)


@click.command()
@click.option('--uri', default='amqp://guest:guest@localhost:5672/%2F', help='RabbitMQ URI')
@click.option('--exchange', default='test-exchange', help='RabbitMQ exchange to publish to')
def start_publishing(uri, exchange):
    '''declare exchange and start publishing messages'''
    connection = pika.BlockingConnection(pika.URLParameters(uri))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type='fanout', auto_delete=True)
    fake = Faker()

    while True:
        try:
            doc = dict(ip=fake.ipv4(), path=fake.uri_path())
            routing_key = generate_routing_key()
            channel.basic_publish(
                exchange, routing_key, json.dumps(doc),
                pika.BasicProperties(content_type='application/json', delivery_mode=pika.spec.TRANSIENT_DELIVERY_MODE)
            )
            time.sleep(0.01)
        except KeyboardInterrupt:
            channel.exchange_delete(exchange=exchange)
            break


if __name__ == '__main__':
    start_publishing()
