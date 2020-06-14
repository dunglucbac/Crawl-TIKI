from concurrent.futures import ThreadPoolExecutor
import psycopg2
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from collections import deque
import pandas as pd

TIKI_URL = 'https://tiki.vn/'

conn = psycopg2.connect(user="postgres",
                        port="5432",
                        database="thuctamdb",
                           password="thuctam")
conn.autocommit = True
cur = conn.cursor()

def create_product_table():
    query = f'''
    CREATE TABLE IF NOT EXISTS products (
       id SERIAL PRIMARY KEY,
       wproductid VARCHAR(255),
       image TEXT,
       fprice INT,
       category VARCHAR(255),
       subcategory VARCHAR(255),
       title VARCHAR(1024),
       seller VARCHAR(255),
       rprice INT,
       discount INT,
       rating INT,
       numreviews INT,
       tikinow INT,
       productlink TEXT,
       cat_id INT,
       created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );'''
    conn = psycopg2.connect(user="postgres",
                        port="5432",
                        database="thuctamdb",
                           password="thuctam")
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(query)
    except Exception as err:
        print(f'ERROR: {err}')
        
# create_product_table()

class Product:
    def __init__(self, prod_id, wproductid, image, fprice, category, subcategory, title, seller, rprice, discount, rating, numreviews, tikinow, productlink, cat_id):
        self.prod_id = prod_id
        self.wproductid = wproductid
        self.image = image
        self.fprice = fprice
        self.category = category
        self.subcategory = subcategory
        self.title = title
        self.seller = seller
        self.rprice = rprice
        self.discount = discount
        self.rating = rating
        self.numreviews = numreviews
        self.tikinow = tikinow
        self.productlink = productlink
        self.cat_id = cat_id
        
    def save_into_db(self):
        
        query = 'SELECT wproductid FROM products WHERE wproductid LIKE %s;'
        val = (self.wproductid,)
        try:
            conn = psycopg2.connect("dbname=thuctamdb user=postgres password=thuctam")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(query, val)
            # print(cur)
            result = cur.fetchall()
            if len(result) > 0:
                conn.close()
                return ''
                
        except Exception as err:
            print(f'SELECT wproductid FROM products WHERE wproductid LIKE {val}: {err}')
            
        query = f"""
            INSERT INTO products (wproductid, image, fprice, category, subcategory, title, seller, rprice, discount, rating, numreviews, tikinow, productlink, cat_id) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id;
        """
        val = (self.wproductid, self.image, self.fprice, self.category, self.subcategory, self.title, self.seller, self.rprice, self.discount, self.rating, self.numreviews, self.tikinow, self.productlink, self.cat_id)
        try:
            cur.execute(query, val)
            # Get id of the new row
            self.prod_id = cur.fetchone()[0]
            conn.close()
        except Exception as err:
            print(f'ERROR INSERT INTO products..{val}: {err}')
        
    def __repr__(self):
        return f'ID: webProductID: {self.wproductid}, ImageURL: {self.image}, FinalPrice: {self.fprice}, MainCategory: {self.category}, SubCategory: {self.subcategory}, Title: {self.title}, Seller: {self.seller}, RegularPrice: {self.rprice}, Discount: {self.discount}, Rating: {self.rating}, NoOfReviews: {self.numreviews}, TikiNOW: {self.tikinow}, ProductLink: {self.productlink}, SubCatID: {self.cat_id}'

def parse(url):
    try:
        response = requests.get(url).text
        response = BeautifulSoup(response, "html.parser")
        return response
    except Exception as err:
        print(f'ERROR: {err}')
        return ''

def get_littlest_cats():
    conn = psycopg2.connect(user="postgres",
                        port="5432",
                        database="thuctamdb",
                           password="thuctam")
    conn.autocommit = True
    cur = conn.cursor()
    query = f"""
    SELECT c.name AS Name, p.url AS ChildURL, p.id AS ChildID, p.parent_id AS ParentID, p.name AS ChildName
        FROM categories AS p LEFT JOIN categories AS c ON c.parent_id = p.id 
        WHERE c.Name IS NULL;
    """
    cur.execute(query)
    result = cur.fetchall()
    conn.close()
    
    return result


littlest_cats = get_littlest_cats()
df = pd.DataFrame(littlest_cats)

df['merge'] = df[1] +' '+ df[2].map(str)
global queue
queue = deque(df['merge'])
# queue[0].split()[1]
# len(queue)


def scrap_product(littlest_cat, articles, k, save_db=False):
    
    try:
        
        #scrape and assign to variables
        wproductid = articles[k]['data-id']
        image = articles[k].img['src']
        fprice = int(articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()[0].strip('đ').replace('.',''))
        rprice = [0 if articles[k].find_all("span",{"class":"price-regular"})[0].text == '' else int(articles[k].find_all("span",{"class":"price-regular"})[0].text.strip('đ').replace('.',''))][0]
        discount = [0 if len(articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()) == 1 else int(articles[k].find_all("span",{"class":"final-price"})[0].text.strip().split()[1].split('%')[0])][0]
        seller = articles[k]['data-brand'].replace('\'','').replace('"','')
        title = articles[k].a['title'].strip().replace('\'','').replace('"','')
        subcategory = articles[k]['data-category'].strip().replace('\'','').replace('"','')
        category = articles[k]['data-category'].strip().replace('\'','').replace('"','').split('/')[0]
        if articles[k].find_all('p',{"class":'review'}) == [] or articles[k].find_all('p',{"class":'review'})[0].text == 'Chưa có nhận xét':
            numreviews = 0
        else:
            numreviews = int(articles[k].find_all('p',{"class":'review'})[0].text.strip('\(\)').split()[0])

        rating = [int(articles[k].find_all('span',{"class":'rating-content'})[0].find('span')['style'].split(':')[1].split('%')[0]) if articles[k].find_all('span',{"class":'rating-content'}) != [] else 0][0]
        tikinow = [0 if articles[k].find_all('i',{"class":"tikicon icon-tikinow-20"}) == [] else 1][0]
        productlink = articles[k].a['href']
        cat_id = int(littlest_cat.split()[1])
        
        product = Product(None, wproductid, image, fprice, category, subcategory, title, seller, rprice, discount, rating, numreviews, tikinow, productlink, cat_id)
        if save_db:
            conn = psycopg2.connect(user="postgres",
                        port="5432",
                        database="thuctamdb",
                           password="thuctam")
            conn.autocommit = True
            cur = conn.cursor()
            product.save_into_db()
            conn.close()
            
    except Exception as err:
        print(err, k)
        

def traverse_and_scrap(littlest_cat):   
    url = littlest_cat.split()[0]
    try:
        soup = parse(url)
        # Read next page cursor at the bottom of a product page
        links = soup.find_all('div',{"class":'list-pager'})
        articles = soup.find_all('div', {"class":'product-item'})
        
        for k in range(len(articles)):
            scrap_product(littlest_cat, articles, k, save_db=True)
        
        while links[0].find_all('a', {"class": "next"}) != []:
            try:
                soup =  parse('https://tiki.vn'+links[0].find_all('a', {"class": "next"})[0]['href'])
                articles = soup.find_all('div', {"class":"product-item"})
                links = soup.find_all('div',{"class":'list-pager'})
                for i in range(len(articles)):
                    scrap_product(littlest_cat, articles, i, save_db=True)
                
            except Exception as e:
                print(f'ERROR traverse and scrap smaller loop: {e}')
                continue
    
    except Exception as err:
        print(f'ERROR traverse and scrap bigger loop: {err}')


def thread_get_sub_from_queue():
    global queue
    if not queue:
        return 'Thread done'
    littlest_cat = queue.popleft()
    traverse_and_scrap(littlest_cat)
    
    thread_get_sub_from_queue()


start = datetime.now()
if __name__ == '__main__':
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.submit(thread_get_sub_from_queue)
        executor.submit(thread_get_sub_from_queue)
        executor.submit(thread_get_sub_from_queue)
        executor.submit(thread_get_sub_from_queue)

end = datetime.now()
print(f'Duration: {end - start}')

# uncomment and run to not threading
# thread_get_sub_from_queue()