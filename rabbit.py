import argparse
import pika
import time


class RabbitMQ:
    def __init__(self):
        self.__connection = None
        self.__channel = None
        self.__queue = ''
        self.__durable = False

    def connect(self):
        self.__connection = pika.BlockingConnection(
            pika.ConnectionParameters('localhost')
        )
        self.__channel = self.__connection.channel()

    def make_persistent_messages(self):
        self.__durable = True

    def set_queue_name(self, queue_name):
        self.__queue = queue_name
        self.__channel.queue_declare(
            queue=queue_name,
            durable=self.__durable
        )

    def send_message(self, message):
        self.__channel.basic_publish(
            exchange='',
            routing_key=self.__queue,
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2
            ) if self.__durable else None
        )

        print(" [x] Sent %r" % message)

    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        time.sleep(body.count(b'.'))
        print(" [x] Done")
        if self.__durable:
            ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_receiver(self):
        print(' [*] Waiting for messages. To exit press CTRL+C')
        if self.__durable:
            self.__channel.basic_qos(prefetch_count=1)
        self.__channel.basic_consume(
            queue=self.__queue,
            on_message_callback=self.callback,
            auto_ack=not self.__durable
        )
        self.__channel.start_consuming()

    def close(self):
        self.__connection.close()


if __name__ == "__main__":
    rabbit = RabbitMQ()
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-d', '--durable', dest='durable', action='store_true')
    arg_parser.add_argument('-q', '--queue', dest='queue')
    arg_parser.add_argument('-r', '--receive', dest='worker', action='store_true')
    arg_parser.add_argument('-s', '--send', dest='task', nargs='+')
    args = arg_parser.parse_args()

    if args.task or args.worker:
        rabbit.connect()

        if args.durable:
            rabbit.make_persistent_messages()

        gl_queue_name = args.queue if args.queue else 'test'
        rabbit.set_queue_name(gl_queue_name)

        if args.task:
            for msg in args.task:
                rabbit.send_message(msg)
            rabbit.close()
        elif args.worker:
            rabbit.start_receiver()




