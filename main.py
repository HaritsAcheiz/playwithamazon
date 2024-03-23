import datetime
import re
import time
from urllib.parse import urljoin
import httpx
from httpx import Client
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import sqlite3
import requests
import pandas as pd

@dataclass
class PlayWithAmazon:
    base_url: str = 'https://www.amazon.de'

    def fetch(self, headers, url):
        conn = sqlite3.connect('amazon.db')
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS product_htmls(
            url TEXT,
            html BLOB
            ) 
            """
        )

        response = requests.get(url, headers=headers)
        time.sleep(1)
        if response.status_code != 200:
            response.raise_for_status()
        html = response.text
        # print(html)
        current = (url, html)
        curr.execute("INSERT INTO product_htmls(url,html) VALUES(?,?)", current)
        conn.commit()

    def truncate_db(self):
        conn = sqlite3.connect("amazon.db")
        curr = conn.cursor()
        curr.execute("DELETE FROM product_datas")
        conn.commit()

    def cek_db(self):
        conn = sqlite3.connect("amazon.db")
        curr = conn.cursor()
        curr.execute("SELECT * FROM product_datas")
        datas = curr.fetchall()
        print(len(datas))

    def drop_db(self):
        conn = sqlite3.connect("amazon.db")
        curr = conn.cursor()
        curr.execute('DROP TABLE IF EXISTS product_datas')
        conn.commit()

    def get_data(self):
        conn = sqlite3.connect("amazon.db")
        curr = conn.cursor()
        curr.execute("SELECT url, html FROM product_htmls")
        datas = curr.fetchall()
        products = []
        for data in datas:
            tree = HTMLParser(data[1])
            timestamp = datetime.datetime.now()
            asin = tree.css_first('div#productDetails_db_sections > div > table > tbody > tr >td').text(strip=True)
            seller_name = tree.css_first('div[data-csa-c-slot-id="odf-feature-text-desktop-merchant-info"]').text(strip=True)
            try:
                pattern = r'seller=([A-Z0-9]+)'
                input_string = tree.css_first('div[data-csa-c-slot-id="odf-feature-text-desktop-merchant-info"] > div > span > a').attributes.get('href')
                match = re.search(pattern, input_string)
                if match:
                    seller_token = match.group(1)
                else:
                    seller_token = ''
            except:
                seller_token = ''
            try:
                price = tree.css_first('span.priceToPay').text(strip=True)
            except:
                price_element = tree.css_first('div#apex_desktop')
                price = price_element.css_first('span.a-price.a-text-price.a-size-medium.apexPriceToPay > span:nth-of-type(1)').text(strip=True)
            try:
                shipping_cost = tree.css_first('span[data-csa-c-type="element"]').attributes.get('data-csa-c-delivery-price')
                delivery_date = tree.css_first('span[data-csa-c-type="element"]').attributes.get('data-csa-c-delivery-time')
            except:
                shipping_cost = ''
                delivery_date = ''
            products.append((asin, timestamp, seller_name, seller_token, price, shipping_cost, delivery_date))

        return set(products)


    def main(self):

        # headers = {
            # 'Host': 'www.amazon.de',
            # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
            # 'Cookie': 'i18n-prefs=EUR;',
            # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            # 'Accept-Language': 'en-US,en;q=0.5',
            # 'Accept-Encoding': 'gzip, deflate, br',
            # 'DNT': '1',
            # 'Sec-GPC': '1',
            # 'Connection': 'keep-alive',
            # 'Upgrade-Insecure-Requests': '1',
            # 'Sec-Fetch-Dest': 'document',
            # 'Sec-Fetch-Mode': 'navigate',
            # 'Sec-Fetch-Site': 'none',
            # 'Sec-Fetch-User': '?1'
        # }

        custom_headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
            'Accept-Language': 'da, en-gb, en',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Referer': 'https://www.google.com/'
        }

        asins = ['B000O6XSFO', 'B0C9Q68TDG', 'B07TL8T5T8', 'B09VPQ5ZTR', 'B0BL3XWYJ5', 'B0035F1DCG', 'B09VC7XWD9', 'B0761VD7L4', 'B09KPWTYNQ', 'B09VP7BZC8']
        self.truncate_db()
        # self.drop_db()
        #
        for asin in asins:
            # client = Client(headers=custom_headers, follow_redirects=True, http2=True)
            endpoint = f'/dp/{asin}?th=1'
            url = urljoin(self.base_url, endpoint)
            self.fetch(headers=custom_headers, url=url)
        #
        # self.cek_db()

        product_datas = list(self.get_data())
        df = pd.DataFrame.from_records(columns=['asin', 'timestamp', 'seller_name', 'seller_token', 'price', 'shipping_cost', 'delivery_date'], data=product_datas)
        df.to_csv('10_amazon_sample_result_rev1.csv', index=False)

        conn = sqlite3.connect("amazon.db")
        curr = conn.cursor()
        curr.execute(
            """
            CREATE TABLE IF NOT EXISTS product_datas(
            asin VARCHAR(255),
            timestamp TIMESTAMP,
            seller_name VARCHAR(255),
            seller_token VARCHAR(255),
            price VARCHAR(255),
            shipping_cost VARCHAR(255),
            delivery_date VARCHAR(255)
            ) 
            """
        )
        for product_data in product_datas:
            curr.execute("INSERT INTO product_datas (asin, timestamp, seller_name, seller_token, price, shipping_cost, delivery_date) VALUES(?,?,?,?,?,?,?)", product_data)
            conn.commit()

if __name__ == '__main__':
    pwa = PlayWithAmazon()
    pwa.main()


