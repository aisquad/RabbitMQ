import argparse
import pika
import time


class RabbitMQ:
    def __init__(self):
        self.__connection = None
        self.__channel = None
        self.__queue = ''

    def connect(self):
        self.__connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.__channel = self.__connection.channel()

    def set_queue(self, queue_name):
        self.__queue = queue_name
        self.__channel.queue_declare(queue=queue_name)

    def send_message(self, message):
        self.__channel.basic_publish(
            exchange='',
            routing_key=self.__queue,
            body=message
        )
        print(" [x] Sent %r" % message)

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        time.sleep(body.count(b'.'))
        print(" [x] Done")

    def start_receiver(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.__channel.basic_consume(
            queue=self.__queue,
            on_message_callback=self.callback,
            auto_ack=True
        )
        self.__channel.start_consuming()

    def close(self):
        self.__connection.close()


if __name__ == "__main__":
    rabbit = RabbitMQ()
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-q', '--queue', dest='queue')
    arg_parser.add_argument('-r', '--receive', dest='receiver', action='store_true')
    arg_parser.add_argument('-s', '--send', dest='send', nargs='+')
    args = arg_parser.parse_args()

    rabbit.connect()

    queue_name = args.queue if args.queue else 'test'
    rabbit.set_queue(queue_name)

    if args.send:
        for message in args.send:
            rabbit.send_message(message)
        rabbit.close()
    elif args.receiver:
        rabbit.start_receiver()
    else:
        rabbit.close()




