import hashlib
from typing import Iterable
from urllib.parse import urlparse
import parsel
import scrapy
import json

from scrapy import Request
from scrapy.cmdline import execute
from marc_jacobs.items import MarcJacobsItem
from marc_jacobs.db_config import config
import pymysql
from datetime import datetime
import os
import gzip
from parsel import Selector
import re

def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value

def generate_hashid(url):
    # Parse the URL and use the netloc and path as a unique identifier
    parsed_url = urlparse(url)
    unique_string = parsed_url.netloc + parsed_url.path
    # Create a hash of the unique string using SHA-256 and take the first 8 characters
    hash_object = hashlib.sha256(unique_string.encode())
    hashid = hash_object.hexdigest()  # Take the first 8 characters
    return hashid

def convert_to_24hr(time_str):
    # Parse the 12-hour format and convert to 24-hour format
    time_obj = datetime.strptime(time_str, '%I%p')  # %I for hour (12-hour), %p for am/pm
    return time_obj.strftime('%H:%M')  # %H for 24-hour, %M for minutes


def get_date(text):
    text = text.replace(': ', ':')
    days = {
        'Mon': 'Monday', 'Tue': 'Tuesday', 'Wed': 'Wednesday',
        'Thu': 'Thursday', 'Fri': 'Friday', 'Sat': 'Saturday', 'Sun': 'Sunday'
    }
    date_parts_list = text.split(' ')
    stri = ''
    date_list = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri']
    if 'Mon-Fri' in date_parts_list[0]:
        date, time = date_parts_list[0].split(':')
        for i in date_list:
            stri += i + ':' + time + ' '
        stri = stri + date_parts_list[1] + ' ' + date_parts_list[2]

    main_stri = ''
    for i in stri.split(' '):
        tx = i.split(':')[0]
        dt = ''
        for j in i.split(':')[-1].split('-'):
            dt += convert_to_24hr(j) + '-'
        dt = tx + ':' + dt[:-1]
        main_stri += dt + ' '

    main_stri = main_stri.strip().replace(' ', ' | ').strip()

    for i in days:
        if i in main_stri:
            # print(i, days[i])
            main_stri = main_stri.replace(i, days[i])

    return main_stri

class MjSpider(scrapy.Spider):
    name = "mj"
    start_urls = ["https://www.marcjacobs.com"]

    def my_print(self, tu):
        for i in tu:
            print(i)
        print('\n')

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)
        self.start_id = start_id
        self.end_id = end_id

        self.conn = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.database,
            autocommit=True
        )
        self.cur = self.conn.cursor()

        self.domain = self.start_urls[0].split('://')[1].split('/')[0]
        self.date = datetime.now().strftime('%d_%m_%Y')

        if 'www' in self.domain:
            self.sql_table_name = self.domain.split('.')[1].replace('-','_') + f'_{self.date}' + '_USA'
        else:
            self.sql_table_name = self.domain.split('.')[0].replace('-','_') + f'_{self.date}' + '_USA'
        self.folder_name = self.domain.replace('.', '_').strip()
        config.file_name = self.folder_name

        self.html_path = 'C:\page_source\\' + self.date + '\\' + self.folder_name + '\\'
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        # print(self.domain, self.folder_name, self.sql_table_name)
        config.db_table_name = self.sql_table_name

        field_list = []
        value_list = []
        item = ('store_no', 'name', 'latitude', 'longitude', 'street', 'city',
                  'state', 'zip_code', 'county', 'phone', 'open_hours', 'url',
                  'provider', 'category', 'updated_date', 'country', 'status',
                  'direction_url', 'pagesave_path')
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        config.fields = ','.join(field_list)
        config.values = ", ".join(value_list)

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.sql_table_name}(id int AUTO_INCREMENT PRIMARY KEY,
                                    store_no varchar(100) DEFAULT 'N/A',
                                    name varchar(100) DEFAULT 'N/A',
                                    latitude varchar(100) DEFAULT 'N/A',
                                    longitude varchar(100) DEFAULT 'N/A',
                                    street varchar(500) DEFAULT 'N/A',
                                    city varchar(100) DEFAULT 'N/A',
                                    state varchar(100) DEFAULT 'N/A',
                                    zip_code varchar(100) DEFAULT 'N/A',
                                    county varchar(100) DEFAULT 'N/A',
                                    phone varchar(100) DEFAULT 'N/A',
                                    open_hours varchar(500) DEFAULT 'N/A',
                                    url varchar(500) DEFAULT 'N/A',
                                    provider varchar(100) DEFAULT 'N/A',
                                    category varchar(100) DEFAULT 'N/A',
                                    updated_date varchar(100) DEFAULT 'N/A',
                                    country varchar(100) DEFAULT 'N/A',
                                    status varchar(100) DEFAULT 'N/A',
                                    direction_url varchar(500) DEFAULT 'N/A',
                                    pagesave_path varchar(500) DEFAULT 'N/A'
                                    )""")

        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9,tr;q=0.8',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.marcjacobs.com/mt-en/stores?srsltid=AfmBOor-dIwxQkAH1btXQYzz7ge5nZ-sXKLdbn55hhtAUOgjFPr0lKrL',
            'sec-ch-ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
        }

    togal = True

    def start_requests(self):
        index = 1
        while self.togal:
            if index%6==0:
                # print('Index :', index)
                index += 1
                url = f'https://www.marcjacobs.com/mt-en/stores?showMap=false&region=NA&countryCode=US&startIndex={index}'
                yield scrapy.Request(
                                        url,
                                        method='GET',
                                        headers=self.headers,
                                        callback=self.parse
                                     )
            else:
                index += 1
    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        list_of_stores = selector.xpath('//*[@id="maincontent"]//section[@class="storelist__content storelist__content-na js-tabs__content storelist__content--active"]//div[@class="store-card g-col-12 g-col-md-4"]')

        print(len(list_of_stores))
        if len(list_of_stores) == 0:
            self.togal = False
        for store in list_of_stores:
            store_url = self.start_urls[0] + store.xpath('.//a[@class="storeDetails-btn"]/@href').get()
            print('URL :', store_url)
            yield scrapy.Request(
                store_url,
                headers=self.headers,
                callback=self.get_store_data
                                 )


    def get_store_data(self, response):
        selector = Selector(response.text)
        item = MarcJacobsItem()

        url = response.url
        print(url)
        store_no = response.url.split('=')[-1]
        name = selector.xpath('//address[@class="detail-address"]/h2/text()').get('N/A')
        direction_url = selector.xpath('//address[@class="detail-address"]/a[contains(text(), "View Directions")]/@href').get('N/A')
        try:
            latitude, longitude = direction_url.split('=')[-1].split(',')
        except Exception as e:
            latitude, longitude = 'N/A', 'N/A'
        street = selector.xpath('//address[@class="detail-address"]/span[@itemprop="streetAddress"]/text()').get('N/A')
        city = selector.xpath('//address[@class="detail-address"]/span[@itemprop="addressLocality"]/text()').get('N/A')
        state = selector.xpath('//address[@class="detail-address"]/span[@itemprop="addressRegion"]/text()').get('N/A')
        zip_code = selector.xpath('//address[@class="detail-address"]/span[@itemprop="postalCode"]/text()').get('N/A')
        country = 'USA'
        phone = selector.xpath('//address[@class="detail-address"]/a[@itemprop="telephone"]/text()').get('N/A')
        county = 'N/A'
        provider = 'Marc Jacobs'
        category = 'Apparel And Accessory Stores'
        updated_date = datetime.now().strftime("%d-%m-%Y")
        status = 'Open'

        try:
            o_h = remove_extra_space(' '.join(selector.xpath('//div[@itemprop="openingHours"]//p//text()').getall()).replace('\n', ''))
            # print(o_h)
            open_hours = o_h
            # open_hours = get_date(o_h)
        except Exception as e:
            open_hours = 'N/A'

        page_id = generate_hashid(store_no)
        pagesave_path = self.html_path + fr'{page_id}' + '.html.gz'

        gzip.open(pagesave_path, "wb").write(response.body)

        item['store_no'] = store_no
        item['name'] = name
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['street'] = street
        item['city'] = city
        item['state'] = state
        item['zip_code'] = zip_code
        item['county'] = county
        item['phone'] = phone
        item['open_hours'] = open_hours
        item['url'] = url
        item['provider'] = provider
        item['category'] = category
        item['updated_date'] = updated_date
        item['country'] = country
        item['status'] = status
        item['direction_url'] = direction_url
        item['pagesave_path'] = pagesave_path
        yield item


if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute(f"scrapy crawl mj -a start_id=0 -a end_id=100 -s CONCURRENT_REQUESTS=6".split())