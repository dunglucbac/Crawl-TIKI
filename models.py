from flask_sqlalchemy import SQLAlchemy
import os
from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)
# from app import db
db = SQLAlchemy(app)

class Product(db.Model):
    __tablename__ = 'products'

    id = db.Column(db.Integer, primary_key=True)
    wproductid = db.Column(db.String())
    image = db.Column(db.String())
    fprice = db.Column(db.String())
    category = db.Column(db.String())
    subcategory = db.Column(db.String())
    title = db.Column(db.String())
    seller = db.Column(db.String())
    rprice = db.Column(db.String())
    discount = db.Column(db.String())
    rating = db.Column(db.String())
    numreviews = db.Column(db.String())
    tikinow = db.Column(db.String())
    productlink = db.Column(db.String())
    cat_id = db.Column(db.String())

    def __init__(self, id, wproductid, image, fprice, category, subcategory, title, seller, rprice, discount, rating, numreviews, tikinow, productlink, cat_id):
        self.id = id
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

    def serialize(self):
        return {
        'id' : self.id,
        'wproductid' : self.wproductid,
        'image' : self.image,
        'fprice' : self.fprice,
        'category' : self.category,
        'subcategory' : self.subcategory,
        'title' : self.title,
        'seller' : self.seller,
        'rprice' : self.rprice,
        'discount' : self.discount,
        'rating' : self.rating,
        'numreviews' : self.numreviews,
        'tikinow' : self.tikinow,
        'productlink' : self.productlink,
        'cat_id' : self.cat_id,
        }

class Category(db.Model):
    __tablename__ = 'categories'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String())
    url = db.Column(db.String())
    parent_id = db.Column(db.String())
    
    def __init__(self, id, name, url, parent_id):
        self.id = id
        self.name = name
        self.url = url
        self.parent_id = parent_id
        
    def save_into_db(self):
        
        query = 'SELECT url FROM categories WHERE url LIKE %s;'
        val = (self.url,)
        try:
            conn = psycopg2.connect("dbname=thuctamdb user=postgres password=thuctam")
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute(query, val)
            result = cur.fetchall()
            if len(result) > 0:
                conn.close()
                return ''
        except Exception as err:
            print(f'ERROR: {err}')
            
        query = f"""
            INSERT INTO categories (name, url, parent_id) 
            VALUES (%s, %s, %s) RETURNING id;
        """
        val = (self.name, self.url, self.parent_id)
        try:
            cur.execute(query, val)
            # Get id of the new row
            self.cat_id = cur.fetchone()[0]
            conn.close()
        except Exception as err:
            print(f'ERROR: {err}')
        
    def __repr__(self):
        return f'ID: {self.id}, Name: {self.name}, URL: {self.url}, Parent ID: {self.parent_id}'        

    def serialize(self):
        return {
            'id': self.id, 
            'name': self.name,
            'url': self.url,
            'parent_id':self.parent_id
        }    