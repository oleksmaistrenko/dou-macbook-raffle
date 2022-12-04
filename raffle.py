import boto3
from datetime import datetime, timedelta
from dateutil import tz
import json
import random
from typing import Any, Sequence, Union
import urllib3
import os
import logging
import time

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
        email, email_domain = email.split('@')
        leave_part = 0.3
        masked_len = len(email) - int(len(email) * leave_part)
        masked_email = email[0:int(len(email) * leave_part)] + ''.join('*' * masked_len)
        masked_email += f'@{email_domain}'
    return masked_email


def get_data_from_jar(token: str, unix_time_from: int, unix_time_to: int, jar_id: str) -> Union[int, Sequence[Any]]:
    '''
    Retrieve the data from Monobank API
    '''
    jar_url = f'https://api.monobank.ua/personal/statement/{jar_id}/{unix_time_from}/{unix_time_to}'
    http = urllib3.PoolManager()
    r = http.request('GET', jar_url, headers={'X-Token': token})
    return r.status, json.loads(r.data.decode('utf-8'))


def select_winner(monobank_data: Sequence[Any], chat_id: str) -> None:
    '''
    Populate slots and select a winner
    '''
    slots = []
    donations = ['person, email, donation time, amount, times included']
    min_amount = 500 * 100
    total = 0
    unique_donators = set()
    biggest_donation = 0
    amount_of_excluded = 0
    amount_of_min_donators = 0

    for el in monobank_data:
        donation = el['amount']
        donation_time = el['time']
        email = ''
        for comment_part in el.get('comment', '').replace('\n', ' ').split(' '):
            if comment_part.find('@') > 0:
                # email = comment_part.split('@')[0]
                email = comment_part
        comment = email
        person = el['description'].split(': ')[1]
        unique_donators.add(person + email)
        total += donation
        if donation > biggest_donation:
            biggest_donation = donation
        times = 0
        if donation >= min_amount:
            times = int(donation / min_amount)
            full_details = f'{person} ({mask_email(comment)}) @ {str(datetime.fromtimestamp(donation_time))}'
            logging.debug(f'{full_details} included {times} time(s)')
            slots.extend([full_details] * times)
            if donation == min_amount:
                amount_of_min_donators += 1
        else:
            amount_of_excluded += 1
            logging.debug(f'{person} unfortunately excluded due to donation of {donation/100:.2f} UAH')
        donations.append(f'{person}, {comment}, {str(datetime.fromtimestamp(donation_time))}, {donation/100}, {times}')

    send_telegram_file(telegram_token, chat_id, 'donations.csv', '\n'.join(donations).encode('utf-8'))
    send_telegram_file(telegram_token, chat_id, 'slots.csv', '\n'.join(slots).encode('utf-8'))

    logging.info(f'Totally raised: {total/100:,.2f} UAH')
    logging.info(f'The biggest donation: {biggest_donation/100:,.2f} UAH')
    logging.info(f'Number of unique donators (by name + email): {len(unique_donators)}')
    logging.info(f'Amount of excluded donators (< {min_amount/100:,.2f} UAH): {amount_of_excluded}')
    logging.info(f'Amount of donators with minimal bet ({min_amount/100:,.2f} UAH): {amount_of_min_donators} ({100 * amount_of_min_donators/len(unique_donators):,.2f}%)')
    logging.info(f'Amount of slots for the raffle: {len(slots)}')
    message = f'Totally raised: {total/100:,.2f} UAH\nThe biggest donation: {biggest_donation/100:,.2f} UAH\n' \
        f'Number of unique donators (by name + email) {len(unique_donators)}\n' \
        f'Amount of excluded donators (< {min_amount/100:,.2f} UAH): {amount_of_excluded}\n' \
        f'Amount of donators with minimal bet ({min_amount/100:,.2f} UAH): {amount_of_min_donators} ({100 * amount_of_min_donators/len(unique_donators):,.2f}%)\n' \
        f'Amount of slots for the raffle: {len(slots)}'
    send_telegram_message(telegram_token, chat_id, message)

    if len(slots) > 0:
        random.seed()
        choosen = random.choice(slots)
        logging.info(f'The winner is {choosen}')
        send_telegram_message(telegram_token, chat_id, f'The winner is {choosen}')
    else:
        logging.info('Not enough of donations bigger than UAH %s', int(min_amount/100))


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
    logging.info('sending message to tg %s', r.data)

def send_telegram_file(telegram_token: str, chat_id: str, file_name: str, file_data: bytes) -> None:
    '''
    send telegram message to the specified chat
    '''
    telegram_url = f'https://api.telegram.org/bot{telegram_token}/sendDocument?chat_id={chat_id}'
    http = urllib3.PoolManager()
    r = http.request_encode_body('POST', telegram_url, fields={'document': (file_name, file_data, 'text/plain')})
    logging.info('sending file to tg %s', r.data)

def lambda_handler(event, context):
    '''
    lamdba handler
    '''
    body = event.get('body')
    if body != None:
        tg_message = json.loads(body.replace('\n', ''))
        logging.info(tg_message)
        username = tg_message['message']['chat']['username']
        chat_id = tg_message['message']['chat']['id']
        text = tg_message['message']['text']
        logging.info(f'got a message from {username}: {text}')
        if username in allowed_users.split(','):
            client = boto3.client('lambda')
            payload = json.dumps({'chat_id': chat_id})
            client.invoke(FunctionName = context.invoked_function_arn, InvocationType='Event', Payload=payload)
    else:
        chat_id = event.get('chat_id')
        eet_tz = tz.gettz('Europe / Kyiv')
        time_paging = timedelta(hours=12)
        # the raffle has been published on 2012-12-2 at 09:00 EET
        start_time = datetime(2022, 12, 2, 0, 0, 0, 0, eet_tz)
        end_time = start_time + time_paging
        today =  datetime(2022, 12, datetime.now().day + 1, 0, 0, 0, 0, eet_tz)
        monobank_data = []
        monobank_limit = 500
        # iterate day by day
        while end_time <= today:
            from_time = int(datetime.timestamp(start_time))
            to_time = int(datetime.timestamp(end_time))
            status, monobank_data_responce = get_data_from_jar(monobank_token, from_time, to_time, jar_id)
            logging.info('responce from mono %s (%s) for %s -> %s', status, len(monobank_data_responce), start_time, end_time)
            if status == 200:
                # in case we've received exact number of the monobank API responce limit
                if len(monobank_data_responce) == monobank_limit:
                    time_paging = time_paging / 2
                    end_time = start_time + time_paging
                else:
                    start_time = end_time
                    end_time = end_time + time_paging
                    monobank_data.extend(monobank_data_responce)
            else:
                # API limit exceeded, wait
                time.sleep(10)
            # logging.info(monobank_data)
        select_winner(monobank_data, chat_id)

    return {}
