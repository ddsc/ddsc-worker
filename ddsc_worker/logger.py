"""Persist (distributed) log messages to a database.

This code will bind a queue named `ddsc.log` to a topic exchange with the same
name. No topic-based filtering is done: all messages will be consumed and
persisted to a database.

"""

from datetime import datetime
import ast

import pika

from ddsc_core.models.logging import LogRecord
from ddsc_worker.celery import celery

EXCHANGE = "ddsc.log"
QUEUE = "ddsc.log"


def callback(ch, method, properties, body):
    # Safely evaluate string to dictionary:
    body = ast.literal_eval(body)
    # Create datetime from float:
    body['time'] = datetime.utcfromtimestamp(body['time'])
    # Persist to database:
    LogRecord(**body).save()
    # Acknowledge:
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    connection = pika.BlockingConnection(
        pika.URLParameters(celery.conf["BROKER_URL"])
    )
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True)
    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key="#")
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(callback, queue=QUEUE)
    channel.start_consuming()


if __name__ == "__main__":
    main()
