import aio_pika

QUEUE_NAME_COMMON = "du_common"
QUEUE_NAME_DEAD = "du_dead"
EXCHANGE_NAME_COMMON = "src_exchange_to_du_common"
EXCHANGE_NAME_DEAD = "dead_letter_exchange"


class RabbitMQHandler:
    """
    В классе RabbitMQHandler создается очередь, точка обмена и пушатся сообщения в очередь.
    Сериальный коннект, созданный на основе класса serial_connection.SerialPortToRabbitMQProcessor,
    будет иметь объект класса RabbitMQHandler, и сможет писать сообщения в очередь через этот объект.
    """
    def __init__(self, worker, connection=None):
        self.connection = connection
        self.worker = int(worker)
        self.exchange = None
        self.queue = None
        self.headers = {'worker': self.worker}

    async def create_queue(self, channel):
        self.exchange = await channel.declare_exchange(EXCHANGE_NAME_COMMON, type="headers", durable=True)
        self.queue = await channel.declare_queue(QUEUE_NAME_COMMON, durable=True,
                                                 arguments={"x-dead-letter-exchange": EXCHANGE_NAME_DEAD,
                                                            "x-queue-mode": "lazy",
                                                            "x-max-priority": 10})
        await self.queue.bind(EXCHANGE_NAME_COMMON, arguments={'x-match': 'all'})

    async def publish_msg(self, msg_body, msg_headers, priority=None):
        self.headers.update(msg_headers)
        message = aio_pika.Message(msg_body, headers=self.headers,  priority=priority,
                                   delivery_mode=aio_pika.DeliveryMode.PERSISTENT)
        await self.exchange.publish(message, routing_key='')
