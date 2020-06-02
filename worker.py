import asyncio
import aio_pika
import aiohttp
import decode
import json
import sys
import re

import mdb
import provider
import payment
import config as cf
import serial_connection

from time import asctime, sleep, time

from etlib import Etisalat

SPORT = 'p2'
ATTEMPT = 0

TIMEOUT = 5
RETRIES = 1

QUEUE_NAME_COMMON = "du_common"
QUEUE_NAME_DEAD = "du_dead"
EXCHANGE_NAME_COMMON = "dst_exchange_to_du_common"
EXCHANGE_NAME_DEAD = "dead_letter_exchange"
TTL_DEAD = 60 * 1000  # 1 секунда


async def get_payment_info(param) -> (dict, str):
    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession() as session:
            url = f"{cf.baseurl}/payment/info/{param}/"
            async with session.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    return await resp.json(), ''
                return {}, ''
    except Exception as e:
        return {}, e


async def recharge(r, sconn) -> dict:
    amo = str(r['amount'])[:-2]
    if r['operator'] == 268:
        ussd = "*139*100*%s*%s*1*" % (cf.duid, cf.dupin) + amo + "*" + r['account'] + "*" + r['account'] + "#"
    elif r['operator'] == 269:
        ussd = "*139*100*%s*%s*8*" % (cf.duid, cf.dupin) + amo + "*" + r['account'] + "*" + r['account'] + "#"
    elif r['operator'] == 270:
        ussd = "*139*100*%s*%s*5*" % (cf.duid, cf.dupin) + amo + "*" + r['account'] + "*" + r['account'] + "#"
    elif r['operator'] == 291:
        ussd = "*139*100*%s*%s*9*" % (cf.duid, cf.dupin) + amo + "*" + r['account'] + "*" + r['account'] + "#"
    else:
        print('[{0}] Wrong DU direction recived: {1}'.format(asctime(), r['operator']))
        return {'status': 'fatal', 'text': 'Wrong DU direction recived: {}'.format(r['operator'])}

    ret = await send(ussd, sconn)
    print('[{0}] DU recharge reply: {1}'.format(asctime(), ret))
    sys.stdout.flush()

    if ret == 'timeout':
        return {'status': 'error', 'text': 'Timeout. Try to retry.'}

    status = re.search('Recharging subscriber .* completed through Operation ([0-9]+)\.', ret)
    if status:
        return {'status': 'ok', 'text': ret, 'transaction': status.group(1)}

    status = re.search('\(Error 1352\) Unknown subscriber\.', ret)
    if status:
        return {'status': 'fatal', 'text': ret}

    status = re.search('\(Error 923\) Service is not available\.', ret)
    if status:
        return {'status': 'error', 'text': ret}

    sys.stdout.flush()
    status = re.search('\(Error 1566\)', ret)
    if status:
        return {'status': 'error_low', 'text': ret}

    status = re.search('\(Error 1565\) UASHttp: Billing failure\.', ret)
    if status:
        return {'status': 'error_low', 'text': ret}
    return {'status': 'fatal', 'text': ret}


async def send(ussd, sconn):
    global q
    """
    eussd = decode.encode(ussd)
    with q.mutex:
        q.queue.clear()

    sconn.write(f'AT+CUSD=1,"{eussd}",15\r\n'.encode())
    # sport.flush()
    timeout = 40

    while timeout > 0:
        if q.empty():
            sleep(1)
            timeout -= 1
            continue
        try:
            r = q.get(False)
        except:
            r = ''
        if r[:5] == '+CUSD':
            return decode.decode(r[10:-6])
        elif r.startswith('ERROR') or r.startswith('+CME ERROR:'):
            return 'timeout'
    """
    return 'timeout'


async def atcmd(cmd, sconn) -> None:
    sconn.write(cmd.encode())
    sconn.write(b'\r\n')
    return


async def balance(sconn) -> dict:
    ussd = '*139*102*%s*%s*2#' % (cf.duid, cf.dupin)
    r = await send(ussd, sconn)
    print('[{0}] DU balance reply: {1}'.format(asctime(), r))
    sys.stdout.flush()
    if r == 'timeout':
        return {'status': 'error', 'text': 'timeout'}
    status = re.search('Your current WOW e-recharge balance is AED ([0-9\.]+).', r)
    if status:
        print('[{0}] DU balance: {1}'.format(asctime(), status.group(1)))
        sys.stdout.flush()
        return {'status': 'ok', 'balance': int(float(status.group(1)))}
    else:
        print('[{0}] DU balance error: {1}'.format(asctime(), r))
        sys.stdout.flush()
        return {'status': 'error', 'text': r}


async def cmgd(id, sconn) -> None:
    sconn.write(f'AT+CMGD={id}\r\n'.encode())
    return


async def cmgr(id, q, sconn) -> None:
    smsd = {}
    ret = ''

    q.queue.clear()
    sconn.write(f'AT+CMGR={id}\r\n'.encode())
    timeout = 15
    while timeout > 0:
        timeout -= 1
    try:
        r = q.get(False)
    except:
        r = ''
    if r.startswith('+CMGR:'):
        smsb = q.get(True, 60)
        print("Process sms")
        sys.stdout.flush()
        # conn.call('queue.tube.mainq:put', {'action': 'sms_du', 'head': r[7:], 'body': smsb})
        sconn.write(f'AT+CMGD={id}\r\n'.encode())
        return
    await asyncio.sleep(1)


async def handle_task_port(data, sconn) -> [str, None]:
    """
    (Функционал из du.py)
    Функция обработки сообщения. В случае успешного выполнения возвращает None.
    B случае ошибки - возвращает текст ошибки, а сообщение помещается в dead_очередь.
    """

    error = ''
    status = ''
    if data['action'] == 'recharge':
        pay2, error = await get_payment_info(data['ticket'])
        if error:
            return error
        elif pay2 and pay2["status"] == "ok":
            status = pay2["data"][4]

        if status == 'Processing' or status == 'Error' or status == 'Prepare':
            ret = await recharge(data, sconn)
            if ret['status'] == 'ok':
                # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
                # conn.call('queue.tube.mainq:put', {'action': 'durecharge', 'ticket': data['ticket'], 'status': ret['status'], 'text': ret['text'], 'transaction': ret['transaction']})
                return
            elif ret['status'] == 'error':
                # tInfo = conn.space(cf.duqueue).select(task[0][0])[0]
                # if (tInfo[6] / 1000000 + tInfo[3] / 1000000 - int(time())) < 310:
                #     conn.call('queue.tube.mainq:put', {'action': 'durecharge', 'ticket': data['ticket'],  'status': 'fatal', 'text': ret['text']})
                #     conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
                # else:
                #     conn.call('queue.tube.%s:release' % (cf.duqueue), task[0][0], {'delay': 300})
                #     conn.call('queue.tube.mainq:put', {'action': 'durecharge', 'ticket': data['ticket'], 'status': ret['status'], 'text': ret['text']})
                return
            elif ret['status'] == 'error_low':
                # conn.call('queue.tube.%s:release' % (cf.duqueue), task[0][0], {'delay': 300, 'pri': 30})
                # conn.call('queue.tube.mainq:put', {'action': 'durecharge', 'ticket': data['ticket'], 'status': 'error', 'text': ret['text']})
                return
            else:
                # conn.call('queue.tube.mainq:put', {'action': 'durecharge', 'ticket': data['ticket'], 'status': ret['status'], 'text': ret['text']})
                # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
                return
        else:
            # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
            pass

    elif data['action'] == 'atcmd':
        await atcmd(data['cmd'], sconn)
        # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
        return

    elif data['action'] == 'balance':
        ret = await balance(sconn)
        if ret['status'] == 'ok':
            # conn.call('queue.tube.mainq:put', {'action': 'dubalance', 'balance': ret['balance']})
            pass
        # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
        return

    elif data['action'] == 'cmgr':
        await cmgr(data['id'], q, sconn)
        # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
        return

    elif data['action'] == 'cmgd':
        await cmgd(data['id'], sconn)
        # conn.call('queue.tube.%s:ack' % (cf.duqueue), task[0][0])
        return

    else:
        # conn.call('queue.tube.%s:bury' % (cf.duqueue), task[0][0])
        pass


async def handle_task_broker(data, worker, pay, prov):
    """
    (Функционал из worker/etisalat.py)
    Функция обработки сообщения. В случае успешного выполнения возвращает None.
    B случае ошибки - возвращает текст ошибки, а сообщение помещается в dead_очередь.
    """

    if data['action'] == 'recharge':
        try:
            status = pay.get(data['ticket'])['status']
        except:
            error = 'Failed database query'  # или возвращать текст ошибки из except-a pay.get()
            return error

        if status == 'Processing' or status == 'Error' or status == 'Prepare':
            if data["amount"] == 0:
                pay.bulkUpdate(data['ticket'], {'lastchange': int(time()),
                                                'status': 'Fatal error', 'text': 'Wrong amount'})
                return
            if len(data["account"]) == 9:
                data["account"] = f"971{data['account']}"

            req = {"amount": int(data["amount"] / 100), "destMsisdn": data["account"], "externalField1": data["ticket"]}
            """
            ret = et.topup(req)
            # ch.basic_ack(method.delivery_tag)
            if 'balance' in ret:
                prov.balance(worker, ret['balance'])
            if ret['status'] == 'error':
                if ATTEMPT < RETRIES:
                    # requeue(ch, 10, json.dumps({'action': 'recharge', 'ticket': data['ticket'], 'amount': data['amount'],
                    #                             'account': data['account']}), hdr['retry'] - 1)
                    pay.bulkUpdate(data['ticket'], {'lastchange': int(time()), 'transaction': ret['tid'],
                                                    'status': 'Error', 'text': ret['text']})
                else:
                    pay.bulkUpdate(data['ticket'], {'lastchange': int(time()), 'transaction': ret['tid'],
                                                    'status': 'Fatal error', 'text': ret['text']})
                error = f'{ret["text"]}'
                return error
            elif ret['status'] == 'ok':
                pay.bulkUpdate(data['ticket'], {'lastchange': int(time()), 'transaction': ret['tid'],
                                                'status': 'Done', 'text': ret['text']})
            else:
                pay.bulkUpdate(data['ticket'], {'lastchange': int(time()), 'transaction': ret['tid'],
                                                'status': 'Fatal error', 'text': ret['text']})
            """
        else:
            error = f'No-action status: {status}'
            return error
    elif data['action'] == 'balance':
        """
        ret = et.balance()
        if ret['status'] == 'ok':
            prov.balance(worker, ret['balance'])
            return
        else:
            # ch.basic_nack(method.delivery_tag, requeue=False)
            return
        """
    else:
        error = f'Unknown action: {data["action"]}'
        return error


def can_retry(message) -> bool:
    """
    Заголовок x-death проставляется при прохождении сообщения через dead_msg_exchange.
    С его помощью можно понять, какой "круг" совершает сообщение.
    """
    deaths = (message.headers or {}).get('x-death')
    if not deaths:
        return True
    if deaths[0]['count'] >= RETRIES:
        return False
    else:
        ATTEMPT = deaths[0]['count']
        return True


async def main(loop):
    # etconf = cf.ETISALAT[sys.argv[1]]
    # et = Etisalat(etconf['KEY'], etconf['POS'], etconf['ID'], etconf['SECRET'], etconf['URL'])
    db = mdb.MDB({"host": cf.MYHOST, "user": cf.MYUSER, "pass": cf.MYPASS, "base": cf.MYDB})
    prov = provider.PROV(db)
    pay = payment.PAY(db)

    serial_conn, serial_config = await serial_connection.create_serial_connection(loop, SPORT)

    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1:5673/", loop=loop)

    async with connection:

        channel = await connection.channel(publisher_confirms=False)
        await channel.set_qos(prefetch_count=1)

        # точка обмена основной очереди
        await channel.declare_exchange(EXCHANGE_NAME_COMMON, type="headers", durable=True)
        # arguments={"x-delayed-type": "headers"})

        # точка обмена для "мертвых" сообщений - отклоненных и отправленных на n-ый круг обработки
        await channel.declare_exchange(EXCHANGE_NAME_DEAD, type="headers", durable=True)

        # основная очередь сообщений, сообщения при nack-е будут попадать в dead_letter_exchange
        common_queue = await channel.declare_queue(QUEUE_NAME_COMMON, durable=True,
                                                   arguments={"x-dead-letter-exchange": EXCHANGE_NAME_DEAD,
                                                              "x-queue-mode": "lazy",
                                                              "x-max-priority": 10})
        await common_queue.bind(EXCHANGE_NAME_COMMON, routing_key='', arguments={"x-match": "all"})

        # очередь для "мертвых" сообщений
        dead_queue = await channel.declare_queue(QUEUE_NAME_DEAD, durable=True,
                                                 arguments={"x-message-ttl": TTL_DEAD,
                                                            "x-dead-letter-exchange": EXCHANGE_NAME_COMMON})
        await dead_queue.bind(EXCHANGE_NAME_DEAD)

        # получаем сообщения из очереди
        async with common_queue.iterator() as queue_iter:
            async for message in queue_iter:
                if message.body is not None:
                    data = json.loads(message.body.decode('utf-8'))
                header = message.headers_raw

                if data:
                    print('[{0}] Got task: {1}{2}'.format(asctime(), data, header))
                    try:
                        if can_retry(message):
                            # даем TIMEOUT на обработку сообщения
                            if header['some_header'] == b'task_from_port':
                                res = await asyncio.wait_for(handle_task_port(data, serial_conn), timeout=TIMEOUT)
                            elif header['some_header'] == b'task_from_broker':
                                res = await asyncio.wait_for(handle_task_broker(data, header['worker'], pay, prov), timeout=TIMEOUT)
                            else:
                                res = "Wrong header"

                            if not res:
                                await message.ack()  # удаляем сообщение
                                print('[{0}] Task done: {1}'.format(asctime(), data))
                            else:
                                print('[{0}] Error occurred in task: {1} {2}'.format(asctime(), data, res))
                                await message.reject()  # помещаем в dead_очередь
                        else:
                            print('[{0}] Task rejected: {1}'.format(asctime(), data))
                            await message.ack()  # удаляем после RETRIES кругов
                            """
                            Тут можно отправить пользователю сообщение об ошибке или c просьбой повторить действие
                            """
                    except asyncio.TimeoutError:
                        print('[{0}] Task timeout: {1}'.format(asctime(), data))
                        await message.reject()  # помещаем в dead_очередь, если обрабатывается дольше, чем TIMEOUT


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
