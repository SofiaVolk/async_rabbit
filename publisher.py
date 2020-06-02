import asyncio
import aio_pika
import traceback
import signal
import sys

import serial_connection as s_c

from RabbitMQHandler import RabbitMQHandler

from time import sleep, asctime


# SPORT = '/dev/serial/by-path/pci-0000:00:1d.0-usb-0:1.2:1.3-port0'
SPORT = 'p2'


def _start_serial_port(sconn):
    sconn.write(b'AT\r\n')
    sys.stdout.flush()
    # sleep(2)
    # sconn.write(b'ATE1\r\n')
    # sleep(2)
    # sconn.write(b'AT+CMGF=1\r\n')
    # sleep(2)
    # sconn.write(b'AT+CNMI=2,1,2,2,0\r\n')


def worker_id():
    if len(sys.argv) != 2:
        print('[{0}] Worker instance absent! Exiting.'.format(asctime()))
        exit()
    return sys.argv[1]


async def main(loop):
    worker = worker_id()
    try:
        connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1:5673/", loop=loop)
    except BaseException as e:
        traceback.print_exc()
        print('[{0}] Catch exception: {1}'.format(asctime(), e))
        sys.exit(255)

    rabbit_handler = RabbitMQHandler(worker, connection)

    async with connection:
        channel = await connection.channel()
        await rabbit_handler.create_queue(channel)

        # создаем асинхронное подключение к serial-порту, содержащее объект с очередью rabbitMQ
        serial_conn, serial_config = await s_c.create_serial_connection(loop, SPORT, s_c.SerialPortToRabbitMQProcessor)
        serial_config.initialize_serial_conn_with_obj(rabbit_handler)
        _start_serial_port(serial_conn)

        await asyncio.sleep(2)
        while connection and serial_conn:
            continue

    # await connection.close()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()

