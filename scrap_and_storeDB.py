import requests
from bs4 import BeautifulSoup
import json
import pandas as pd
import psycopg2

def load_website(url,prefix):
    try:
        response = requests.get(prefix+url)
        return BeautifulSoup(response.text)
    except Exception as err:
        print(f'ERROR: {err}')

def scrape_and_insert(cat, j, articles, k, cur, conn, tablename):
    #Works for scraping beautiful soup product item from tiki product page
    try:
        #scrape and assign to variables
        images = articles[k].img['src']
        fprice = articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()[0]
        rprice = articles[k].find_all("span",{"class":"price-regular"})[0].text
        discount = ['None' if len(articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()) == 1 else articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()[1]][0]
        seller = articles[k]['data-brand']
        titles = articles[k].a['title'].strip().replace('\'','').replace('"','')
        subcategory = articles[k]['data-category'].strip()
        category = cat[j][0]
        num_reviews = [articles[k].find_all('p',{"class":'review'})[0].text.strip('\(\)') if articles[k].find_all('p',{"class":'review'}) != [] else 'Chưa có nhận xét'][0]
        ratings = [articles[k].find_all('span',{"class":'rating-content'})[0].find('span')['style'].split(':')[1] if articles[k].find_all('span',{"class":'rating-content'}) != [] else 'Rating not available'][0]
        tikinow = ['NO' if articles[k].find_all('i',{"class":"tikicon icon-tikinow-20"}) == [] else 'YES'][0]
        productlink = articles[k].a['href']
        #build query string
        query = f"""INSERT INTO {tablename}(images, fprice, category, subcategory, titles, seller, rprice, discount, ratings, num_reviews, tikinow, productlink)
                    VALUES('{images}','{fprice}', '{category}', '{subcategory}', '{titles}','{seller}','{rprice}','{discount}','{ratings}','{num_reviews}','{tikinow}','{productlink}');"""
        #commit to connection
        cur.execute(query)
        conn.commit()
    except Exception as err:
        print(err)

#load tiki home page
soup = load_website('https://tiki.vn/',prefix='')
#scrape the categories and their links and store in array
categories = soup.find_all('a',{"class":'MenuItem__MenuLink-tii3xq-1 efuIbv'})
category, link = [], []
for h in range(len(categories)):
    try:
        link.append(categories[h]['href'])
        category.append(categories[h].text)
    except:
        print('pass')
cat = list(zip(category,link))

#create connect to DB and cursor
conn = psycopg2.connect("dbname=thuctamdb user=postgres password=thuctam")

cur = conn.cursor()

#create new table
tablename = 'products'
query = f'''
CREATE TABLE {tablename}(
   id SERIAL PRIMARY KEY,
   images VARCHAR(1024),
   fprice VARCHAR(1024),
   category VARCHAR(1024),
   subcategory VARCHAR(1024),
   titles VARCHAR(1024),
   seller VARCHAR(1024),
   rprice VARCHAR(1024),
   discount VARCHAR(1024),
   ratings VARCHAR(1024),
   num_reviews VARCHAR(1024),
   tikinow VARCHAR(1024),
   productlink VARCHAR(1024)
);'''
cur.execute(query)
conn.commit()

for j in range(len(cat)):
    try:
        soup = load_website(cat[j][1],prefix='')
        articles = soup.find_all('div', {"class":'product-item'})
        print('Reading '+cat[j][1])
        for k in range(len(articles)):
            scrape_and_insert(cat, j, articles, k, cur, conn, tablename)
                
        # Read next page cursor at the bottom of a product page        
        links = soup.find_all('div',{"class":'list-pager'})  
        
        #While next page cursor is not empty, read next page cursor to move to next product page
        while links[0].find_all('a', {"class": "next"}) != []:
            try:
                soup = load_website(links[0].find_all('a', {"class": "next"})[0]['href'],prefix='https://tiki.vn')
                articles = soup.find_all('div', {"class":"product-item"})
                print('Reading',cat[j][0],links[0].find_all('a', {"class": "next"})[0]['href'].split('&')[1],sep=' ')
                for i in range(len(articles)):
                    scrape_and_insert(cat, j, articles, i, cur, conn, tablename)
                links = soup.find_all('div',{"class":'list-pager'})
            except:
                continue
    except:
        continue
print("SUCCESS!")

query = f'SELECT * FROM {tablename} ORDER BY id DESC LIMIT 5'
cur.execute(query)
cur.fetchall()