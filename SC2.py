import asyncio
import serial_asyncio
import sys

from pandas.io import json

SPORT = 'pp1'


class Output1(asyncio.Protocol):

    def __init__(self):
        self._transport = None
        self.data = None

    def connection_made(self, transport: serial_asyncio.SerialTransport):
        self._transport = transport
        print('port opened p1')

    def data_received(self, data):
        print('data received', repr(data))
        msg_body = json.dumps({'action': 'recharge', 'amount': 'send', 'counter': 1}).encode('utf-8')
        self._transport.write(msg_body)
        sys.stdout.flush()
        # self._transport.close()

    def connection_lost(self, exc):
        print('port closed')
        asyncio.get_event_loop().stop()


async def create_serial_connection(loop, serial_port=SPORT):
    return await serial_asyncio.create_serial_connection(loop, Output1, serial_port, baudrate=115200, rtscts=True)


async def main(loop):
    await create_serial_connection(loop, SPORT)
    await asyncio.sleep(120)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
