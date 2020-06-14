import os
from flask import Flask, request, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config.from_object("config.DevelopmentConfig")
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

from models import Product, Category

@app.route("/")
def hello():
    return render_template('start.html')

@app.route("/product/getall")
def get_all():
    try:
        product=Product.query.all()
        products = [dict(e.serialize()) for e in product]
        return render_template('home.html',products = products)
    except Exception as e:
	    return(str(e))

@app.route("/product/getid/<id_>")
def get_prod_by_id(id_):
    try:
        product=Product.query.filter_by(id=id_).first()
        products = [dict(product.serialize())]
        return render_template('home.html',products=products)
        
    except Exception as e:
	    return(str(e))

@app.route("/product/getseller/<seller_>")
def get_prod_by_seller(seller_):
    try:
        product=Product.query.filter_by(seller=seller_)
        products = [dict(e.serialize()) for e in product]
        return render_template('home.html',products=products)
        
    except Exception as e:
	    return(str(e))


@app.route("/product/getcategory/<catid_>")
def get_prod_by_category(catid_):
    try:
        product=Product.query.filter_by(cat_id=catid_)
        products = [dict(e.serialize()) for e in product]
        return render_template('home.html',products=products)
        
    except Exception as e:
	    return(str(e))

@app.route("/product/<sql_>")
def get_prod_by_sql(sql_):
    try:
        if 'drop' not in sql_.lower() and 'insert' not in sql_.lower() and 'delete' not in sql_.lower() and 'create' not in sql_.lower():
            result = db.engine.execute(sql_)
            return render_template('home.html',products=result)
        else:
            return render_template('error.html')
        
    except Exception as e:
	    return(str(e))

@app.route("/category/getid/<id_>")
def get_cat_by_id(id_):
    try:
        cat=Category.query.get(id_)
        cats = [dict(cat.serialize())]
        return render_template('category.html',products=cats)
        
    except Exception as e:
	    return(str(e))

@app.route("/category/<sql_>")
def get_cat_by_sql(sql_):
    try:
        if 'drop' not in sql_.lower() and 'insert' not in sql_.lower() and 'delete' not in sql_.lower() and 'create' not in sql_.lower():
            result = db.engine.execute(sql_)
            return render_template('category.html',products=result)
        else:
            return render_template('error.html')
        
    except Exception as e:
	    return(str(e))        

@app.route("/presentation")
def present():
    return render_template('presentation.html')

if __name__ == '__main__':
    app.run()
