#from cgitb import html
#from email.mime import image
#from http.client import CannotSendRequest
#from unicodedata import name
import psycopg2
import requests
from bs4 import BeautifulSoup
import csv
import os
import airflow
import datetime as dt
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

URL = 'https://auto.ria.com/uk/newauto/marka-honda/'
HEADERS = ()
FILE = 'cars.csv'

def get_html (url, params=None):
    r = requests.get(url,params)
    return r

def get_pages_count (html):
    soup= BeautifulSoup(html, 'html.parser')
    page = soup.find_all('span', class_ = 'page-item mhide')
    if page:
        return int(page[-1].get_text())
    else:
        return 1
		
def get_content(html):
    soup = BeautifulSoup (html, 'html.parser')
    items = soup.find_all('section', class_='proposition')
    print (len(items))

    cars=[]
    for item in items:
        cars.append({
            'title': item.find('span', class_='link').get_text(strip=True),
            'link': 'https://auto.ria.com' + item.find('a', class_='proposition_link').get('href'),
            'usd_price': item.find('span', class_='green bold size22').get_text().strip(),
            'uah_price': item.find('span', class_='size16').get_text(),
            'city': item.find('span', class_='item region').get_text(),
            'image': item.find('img').get('src'),
            })
    return cars		

def save_file(items, path):
    with open(path, 'w', newline='') as file:
        writer = csv.writer(file, delimiter=';')
        writer.writerow(['MARKA','LINK','PRICE_IN_USD', 'PRICE_IN_UAH','CITY','PHOTO_LINK'])
        for item in items:
            writer.writerow([item['title'], item['link'],item['usd_price'],item['uah_price'],item ['city'],item['image']])
       
        name_file = ''
        for i in items:
            name_file=  'cars/' + ''.join((i['image'].split('/')[-1:]))
            photo = requests.get(i['image'],params=None).content
            with open(name_file, 'wb') as f:
                f.write(photo)
            print(name_file)



dag = DAG(
 dag_id="download_cars_test",
 start_date=dt.datetime(2022, 9, 21),
 schedule_interval=None,
)

def connect():
    html = get_html(URL)
    if html.status_code == 200:
        cars=[]
        page_count=get_pages_count(html.text)
        for page in range(1,page_count+1):
            print(f'Парсінг сторінки {page} із {page_count}...')
            html=get_html(URL, params={'page':page})
            cars.extend(get_content(html.text))
        save_file(cars, FILE)
        os.startfile(FILE)
        
        print(len(cars))
    else:
        print('error_connetion')
    
    
def save_to_database():
    conn = psycopg2.connect(dbname='database', user='db_user', 
                        password='mypassword', host='localhost')
	cursor = conn.cursor()
	cursor.execute('INSERT INTO books (title, link, usd_price, uah_price, city, image) 
				VALUES ('Volvo XC40 2021', 'https://auto.ria.com/newauto/auto-volvo-xc40-1872861.html' 49641, 2081461, 'Одеса', 'https://auto.ria.com/newauto/auto-volvo-xc40-1872861.html');')
					


connect = PythonOperator(
    task_id="get_connect_to_html",
    python_callable=connect,
    dag=dag,
)

save_to_database = PythonOperator(
    task_id="save_data_to_database",
    python_callable=save_to_database,
    dag=dag,
)

connect>>save_to_database

