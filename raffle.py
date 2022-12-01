from datetime import datetime
import json
import random
from typing import Any, Sequence, Union
import urllib3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

monobank_token = os.environ.get('monobank_token')
jar_id = os.environ.get('monobank_jar_id')
allowed_users = os.environ.get('users', 'oleksm')
telegram_token = os.environ.get('telegram_token')

def mask_email(email: str) -> str:
    '''
    Mask the last symbols of email
    '''
    masked_email = email
    if len(email) > 0:
        leave_part = 0.6
        masked_len = len(email) - int(len(email) * leave_part)
        masked_email = email[0:int(len(email) * leave_part)] + ''.join('*' * masked_len)
    return masked_email


def get_data_from_jar(token: str, unix_time: int, jar_id: str) -> Union[int, Sequence[Any]]:
    '''
    Retrieve the data from Monobank API
    '''
    jar_url = f'https://api.monobank.ua/personal/statement/{jar_id}/{unix_time}'
    http = urllib3.PoolManager()
    r = http.request('GET', jar_url, headers={'X-Token': token})
    return r.status, json.loads(r.data.decode('utf-8'))


def select_winner(monobank_data: Sequence[Any]):
    '''
    Populate slots and select a winner
    '''
    slots = []
    min_amount = 500 * 100
    total = 0
    unique_donators = set()
    biggest_donation = 0

    for el in monobank_data:
        donation = el['amount']
        donation_time = el['time']
        email = ''
        for comment_part in el.get('comment', '').replace('\n', ' ').split(' '):
            if comment_part.find('@') > 0:
                email = comment_part.split('@')[0]
        comment = email
        person = el['description'].split(': ')[1]
        unique_donators.add(person)
        total += donation
        if donation > biggest_donation:
            biggest_donation = donation
        if donation >= min_amount:
            times = int(donation / min_amount)
            full_details = f'{person} ({mask_email(comment)}) @ {str(datetime.fromtimestamp(donation_time))}'
            logging.info(f'Thanks, {full_details:<80} included {times} time(s)')
            slots.extend([full_details] * times)
        else:
            logging.info(f'Thanks, {person:<80} unfortunately excluded due to donation of {donation/100:.2f} UAH')

    logging.info(f'\n{"Totally raised:":<32}{total/100:,.2f} UAH')
    logging.info(f'{"Biggest donation:":<32}{biggest_donation/100:,.2f} UAH')
    logging.info(f'{"Number of unique donators:":<32}{len(unique_donators)}')
    logging.info(f'{"Number of slots:":<32}{len(slots)}')

    if len(slots) > 0:
        random.seed()
        choosen = random.choice(slots)
        logging.info(f'\n{"Selected person:":<32}{choosen}')
    else:
        logging.info(f'Not enough of donations bigger than UAH {int(min_amount/100)}')


def send_telegram_message(telegram_token: str, chat_id: str, message: str) -> None:
    '''
    send telegram message to the specified chat
    '''
    telegram_url = f'https://api.telegram.org/bot{telegram_token}/sendMessage'
    data = {'chat_id': chat_id, 'text': message}
    data = json.dumps(data).encode()
    headers = {'Content-Type': 'application/json'}
    http = urllib3.PoolManager()
    r = http.request('POST', telegram_url, body=data, headers=headers)
    logging.info(r.status)

def lambda_handler(event, context):
    '''
    lamdba handler
    '''
    body = event['body']
    tg_message = json.loads(body.replace('\n', ''))
    logging.info(tg_message)
    username = tg_message['message']['chat']['username']
    chat_id = tg_message['message']['chat']['id']
    text = tg_message['message']['text']
    logging.info(f'got a message from {username}: {text}')

    if username in allowed_users.split(','):
        unix_timestamp = 1669845600
        status, monobank_data = get_data_from_jar(monobank_token, unix_timestamp, jar_id)
        logging.info(status)
        logging.info(monobank_data)
        select_winner(monobank_data)

        send_telegram_message(telegram_token, chat_id, f'hello, @{username}')

    return {}
