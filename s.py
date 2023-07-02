import requests
from bs4 import BeautifulSoup
import json
from time import sleep, time
import sqlite3
import sched


"""Scheduling is implemented using the built-in sched module.
Initialize the scheduler, schedule an event using enter()
Create a recursion by calling the scheduler inside the function.
"""

HEADERS = {'User-Agent':'Mozilla/5.0 (Windows NT 10.0)'}
INTERVAL = 0 # actual interval set within the collect_data function
API_TOKEN = ''
CHAT_ID = ''
API_URL = f'https://api.telegram.org/bot{API_TOKEN}/sendMediaGroup'

def handle_response(media):
    while True:
        response = requests.post(API_URL, json={
            'chat_id': CHAT_ID, 'media': media})
        r_json = response.json()
        if r_json['ok'] == False:
            if 'retry after' in r_json['description']:
                timeout = r_json['parameters']['retry_after'] + 5
                print(f'(!) too many requests, sleeping for {timeout} seconds')
                sleep(timeout)
                continue
            elif 'Wrong type of the web page' in r_json['description']:
                list_desc = list(r_json['description'])
                bad_index = int(list_desc[list_desc.index('#') + 1]) - 1
                media_send_correct = json.loads(media)
                del media_send_correct[bad_index]
                media = json.dumps(media_send_correct)
                print(f'(!) retrying to send listing {car_id}')
                sleep(2)
                continue
            else:
                print(f'(!) error with listing {car_id}, investigate\n' + \
                    str(media) + '\n' + str(r_json))
                sleep(2)
                continue
        else:
            break

def collect_detailed(data, ids_in_db):
    for data_item in data:
        car_id = data_item[0]
        if car_id not in ids_in_db:
            link = data_item[1]
            price = data_item[2]
            page_data = requests.get(link)
            page_data_load = BeautifulSoup(page_data.content, 'html.parser')

            make = page_data_load.select('span.label:-soup-contains("Марка")'\
                ' + span.argument.d-link__name')[0].getText()
            photos = [photo['src'] for photo in page_data_load.select(
                'div.carousel-inner img.outline.m-auto')][:-1]
            bidfax_link = page_data_load.select(
                'script[data-bidfax-pathname]')[0]['data-bidfax-pathname'].\
            replace('/bidfax/', 'https://bidfax.info/')
            dmg_data_link = 'https://webcache.googleusercontent.com/search?q='\
            f'cache:{bidfax_link}'
            dmg_data = requests.get(dmg_data_link)
            dmg_data_load = BeautifulSoup(dmg_data.content, 'html.parser')
            photos_dmg = ['https://bidfax.info' + item['src'] for item in \
            dmg_data_load.select('ul.xfieldimagegallery.skrin img')]
            mileage = json.loads(page_data_load.select(
                'script:-soup-contains("mileageFromOdometer")')[0].\
            getText())['mileageFromOdometer']['value']
            city = page_data_load.select(
                'a[href*="/legkovie/city/"]')[0]['href'][18:-1].capitalize().\
            replace('-', ' ')

            media_list = [{'type':'photo', 'media':photo} \
            for photo in photos[:10]]
            media_list[0].update({'parse_mode':'MarkdownV2',
                'caption':f'[{make}]({link})\n\U0001F194 {car_id}\n'\
                f'${price}\n{mileage} km\n{city}\n[bidfax]({bidfax_link})'})
            media_send = json.dumps(media_list)

            data_full = (car_id, link, price, make, '----'.join(photos),
                bidfax_link, mileage, city, '----'.join(photos_dmg), 'ACTIVE')

            c.execute('''
                INSERT INTO cars (car_id, link, price, make, photos,
                bidfax_link, mileage, city, photos_dmg, status)
                    VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                      ''', data_full)

            handle_response(media_send)
            if len(photos_dmg) > 0:
                sleep(1)
                media_list_dmg = [{'type':'photo', 'media':photo} for photo in \
                photos_dmg[:10]]
                media_list_dmg[0].update({'parse_mode':'MarkdownV2',
                    'caption':f'\U0001F194 {car_id}'})
                media_send_dmg = json.dumps(media_list_dmg)
                handle_response(media_send_dmg)

            print(f'listing {car_id} scraped')
            conn.commit()
            sleep(2)

def collect_data(sc):
    INTERVAL = 600 # scheduler interval in seconds
    c.execute('''
    SELECT car_id, price, status FROM cars;
              ''')

    ids_prices_statuses_db = []
    for row in c.fetchall():
        ids_prices_statuses_db.append(list(row))
    ids_prices_db = [item[:-1] for item in ids_prices_statuses_db]
    ids_in_db = [item[0] for item in ids_prices_statuses_db]

    data = []
    page = 0
    while True:
        page_data = requests.get(
            'https://auto.ria.com/uk/search/'
            '?indexName=auto,order_auto,newauto_search&categories.main.id=1'
            '&brand.id[0]=79&model.id[0]=2104&country.import.usa.not=0'
            '&price.currency=1&abroad.not=0&custom.not=1&damage.not=0'
            f'&page={str(page)}&size=20', headers=HEADERS)
        page_data_load = BeautifulSoup(page_data.content, 'html.parser')

        cards = page_data_load.select('section.ticket-item')
        if len(cards) != 0:
            for card in cards:
                car_id = int(card['data-advertisement-id'])
                link = card.select('a.address')[0]['href']
                price = int(card.select('span.bold.size22.green'\
                    '[data-currency="USD"]')[0].getText().replace(' ', ''))
                data.append([car_id, link, price])
            page += 1
            print(f'page {page} scraped')
            sleep(2)
        else:
            break

    API_URL_MSG = API_URL.replace('sendMediaGroup', 'sendMessage')
    data_sans_links = [[item[0], item[2]] for item in data]
    for item in data_sans_links:
        if item not in ids_prices_db and item[0] in ids_in_db:
            c.execute('''
            SELECT price FROM cars
            WHERE car_id = ?;
                      ''', (item[0],))
            old_price = c.fetchall()[0][0]
            c.execute('''
            UPDATE cars
            SET price = ?
            WHERE car_id = ?;
                      ''', (item[1], item[0]))
            conn.commit()
            response = requests.post(API_URL_MSG, json={'chat_id': CHAT_ID,
                'text': f'\U0001F194 {item[0]} - new price (${item[1]}, '\
                f'from ${old_price})'})
            sleep(2)

    ids_in_data = [item[0] for item in data_sans_links]
    for item in ids_prices_statuses_db:
        if item[:-1] not in data_sans_links and item[0] not in ids_in_data and \
        item[2] == 'ACTIVE':
            c.execute('''
            UPDATE cars
            SET status = 'DELISTED'
            WHERE car_id = ?;
                      ''', (item[0],))
            conn.commit()
            response = requests.post(API_URL_MSG, json={'chat_id': CHAT_ID,
                'text': f'\U0001F194 {item[0]} has been sold (delisted)'})
            sleep(2)

    collect_detailed(data, ids_in_db)

    sc.enter(INTERVAL, 1, collect_data, (sc,))

conn = sqlite3.connect('data.db')
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS cars (
        car_id INTEGER PRIMARY KEY,
        link TEXT,
        price INTEGER,
        make TEXT,
        photos TEXT,
        bidfax_link TEXT,
        mileage INTEGER,
        city TEXT,
        photos_dmg TEXT,
        status TEXT
    );
          ''')

s = sched.scheduler(time, sleep)
s.enter(INTERVAL, 1, collect_data, (s,))
s.run()
