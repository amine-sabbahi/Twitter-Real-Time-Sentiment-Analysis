from flask import Flask, jsonify, render_template
from pymongo import MongoClient
from bson.json_util import dumps
import os

app = Flask(__name__)

mongo_host = os.getenv('MONGO_HOST', 'localhost')
mongo_client = MongoClient(f'mongodb://{mongo_host}:27017')
db = mongo_client['twitter_db']
collection = db['twitter_collection']

@app.route('/data')
def get_data():
    cursor = collection.find().sort('_id', -1).limit(20)  # Fetch the latest 100 entries
    return dumps(cursor)

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(debug=True)
