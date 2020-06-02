import asyncio
import serial_asyncio
import traceback
import json
import sys
import re

import RabbitMQHandler

from time import asctime, sleep

# SPORT = '/dev/serial/by-path/pci-0000:00:1d.0-usb-0:1.2:1.3-port0'
SPORT = 'p2'


class SerialPortProcessor(asyncio.Protocol):
    """
    Базовый класс, обспечивающий асинхронную работу записи-чтения serial-порта
    """
    def __init__(self):
        self._transport = None

    def connection_made(self, transport: serial_asyncio.SerialTransport):
        self._transport = transport
        print('port opened')

    # def data_received(self, data):
    #     print('data received', repr(data))
    #     self._transport.close()

    def connection_lost(self, exc):
        print('port closed')
        asyncio.get_event_loop().stop()


class SerialPortToRabbitMQProcessor(SerialPortProcessor):
    """
    Модификация базового класса, содержащая объект класса RabbitMQHandler.
    Он позволяет пушить сообщения в очередь RabbitMQ.
    """
    def __init__(self):
        SerialPortProcessor.__init__(self)
        self.data = None
        self.rabbitMQ_obj = None

    def initialize_serial_conn_with_obj(self, rabbit_mq_obj: RabbitMQHandler):
        print("rabbit internal inited")
        self.rabbitMQ_obj = rabbit_mq_obj

    def data_received(self, data):
        print('data received', repr(data))
        asyncio.ensure_future(self._serial_loop(data))
        # self._transport.close()

    async def _serial_loop(self, data):
        # headers = {'some_header': 'task_from_port'}
        # msg = json.dumps({'action': 'recharge', 'operator': '33333', 'amount': 'qwerty'}).encode('utf-8')
        # await self.rabbitMQ_obj.publish_msg(msg, headers)
        smsd = {}
        try:
            data = data.decode()
            if data[:6] == '+CMTI:':
                # sms = re.search('\+CMTI: "..",([0-9]+)', data)
                sms = re.search('\+CMTI:', data)
                if sms:
                    # conn.call('queue.tube.%s:put' % (cf.dulocal), {'action': 'cmgr', 'id': sms.group(1)}, {'pri': 5})
                    message = json.dumps({'action': 'cmgr', 'id': sms.group(1)}).encode('utf-8')
                    headers, priority = {'some_header': 'task_from_port'}, 5
                    await self.rabbitMQ_obj.publish_msg(message, headers, priority)
            elif data.startswith('^SRVST:2'):
                # sleep(60)
                # ser.write('AT+CNMI=2,1,2,2,0')
                self._transport.write(b'AT+CNMI=2,1,2,2,0')
                # sleep(10)
            elif data[:6] == '+CMGL:':
                sms = re.search('\+CMGL: ([0-9]+),(.*)', data)
                # smsd['head'] = sms.group(2)
                # smsd['body'] = ser.readline().decode() # обработать отдельно
                # conn.call('queue.tube.%s:put' % (cf.dulocal), {'action': 'cmgd', 'id': s.group(1)}, {'pri': 5})
                if sms:
                    message = json.dumps({'action': 'cmgd', 'id': sms.group(1)}).encode('utf-8')
                    headers, priority = {'some_header': 'task_from_port'}, 5
                    await self.rabbitMQ_obj.publish_msg(message, headers, priority)
                # conn.call('queue.tube.mainq:put', {'action': 'sms_du', 'head': smsd['head'], 'body': smsd['body']})
        except:
            traceback.print_exc()
            print('[{0}] Catch exception: {1}'.format(asctime(), sys.exc_info()[1]))
            sys.exit(255)


async def create_serial_connection(loop, serial_port, protocol_factory=SerialPortProcessor, baudrate=115200):
    return await serial_asyncio.create_serial_connection(loop, protocol_factory, serial_port,
                                                         baudrate=baudrate, rtscts=True)
