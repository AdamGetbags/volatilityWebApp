"""
Volatility web app + images - polygon API
@author: AdamGetbags
"""

# Import modules
from flask import Flask, render_template, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_marshmallow import Marshmallow
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, Float
from APIkeys import polygonAPIkey
from polygon import RESTClient
import pandas as pd
import numpy as np
import sqlite3
import os

basedir = os.path.abspath(os.path.dirname(__file__))

# Create flask app instance
vol_app = Flask(__name__)
vol_app.config["SQLALCHEMY_DATABASE_URI"] = 'sqlite:///' + os.path.join(basedir, 'vol_app.db')

db = SQLAlchemy(vol_app)
ma = Marshmallow(vol_app)

# Create client and authenticate w/ API key // rate limit 5 requests per min
client = RESTClient(polygonAPIkey) # api_key is used

# Declare model
class VolData(db.Model):
    __tablename__ = 'vol_table'
    id = db.Column(db.Integer, primary_key=True)
    ticker = db.Column(db.String, nullable=True, unique=True)
    std_dev = db.Column(db.Float, nullable=True)
    image_url = db.Column(db.String, nullable=True)

    def __init__(self, ticker, std_dev, image_url) -> None:
        super(VolData, self).__init__()
        self.ticker = ticker
        self.std_dev = std_dev
        self.image_url = image_url

    def __repr__(self) -> str:
        return '<VolData %r>' % self.ticker

# Schema
class VolDataSchema(ma.Schema):
    class Meta:
        fields = ('id', 'ticker', 'std_dev', 'image_url')

single_vol_data_schema = VolDataSchema()
multiple_vol_data_schema = VolDataSchema(many=True)

# Create tables/db file
with vol_app.app_context():
    db.create_all()


# Main url
@vol_app.route('/', methods=['GET'])
def create_main():

    all_data = VolData.query.all()
    all_data_ser = multiple_vol_data_schema.dump(all_data)
    all_data_df = pd.DataFrame(all_data_ser)

    print(all_data_df)
    
    return render_template('index.html', volatility_data=all_data_df)


# Add vol data entry to database
@vol_app.route('/data', methods=['POST'])
def add_vol_data():
    stock_tickers = request.json['tickers']
    
    # Empty dataframe to store log returns
    std_devs_data = pd.DataFrame()

    for i in stock_tickers:
        print('processing ' + i)
        
        try:
            # Request daily bars
            data_request = client.get_aggs(ticker = i, 
                                        multiplier = 1,
                                        timespan = 'day',
                                        from_ = '2022-09-01',
                                        to = '2023-03-25')
            
            # List of polygon agg objects to DataFrame
            price_data = pd.DataFrame(data_request)
            
            # Create Date column
            price_data['Date'] = price_data['timestamp'].apply(
                                    lambda x: pd.to_datetime(x*1000000))
            

            price_data = price_data.set_index('Date')
                
            price_data['log_returns'] = np.log(price_data.close) - np.log(
                price_data.close.shift(1))
            
            # Rolling stdDev window
            rolling_std_dev_window = 20
            
            # Rolling stdDev log returns 
            price_data['std_devs'] = price_data['log_returns'].rolling(
                center=False, window = rolling_std_dev_window).std()
            
            std_devs_data[i] = price_data.std_devs

        except Exception as e:
            print(str(e) + 'error on ticker ' + i) 
            continue

    # Trim data
    std_devs_data = std_devs_data[rolling_std_dev_window:]

    # Rename index before transpose
    std_devs_data.index = std_devs_data.index.rename('idx')

    # Transpose data
    sorted_data = std_devs_data[-1:].T

    # Reset index
    sorted_data.reset_index(inplace=True)

    # Rename column
    sorted_data = sorted_data.rename(
        columns={sorted_data.columns[0]: "tickers",
                    sorted_data.columns[1]: "std_devs"})

    # Sort data
    sorted_data = sorted_data.sort_values(by=['std_devs'], 
                                            ascending=False,
                                            ignore_index=True)
                    
    # Add urls to dataframe            
    for j in sorted_data['tickers']:
        iconUrl = client.get_ticker_details(ticker=j).branding.icon_url
        iconUrl = iconUrl + '?apiKey=' + polygonAPIkey
        sorted_data.loc[sorted_data['tickers'] == j, 'image_urls'] = iconUrl
            
    # print(sorted_data)

    entry_list = []

    for k in sorted_data.itertuples(index=False):
        new_entry = VolData(k.tickers, 
                            k.std_devs,
                            k.image_urls)
        entry_list.append(new_entry)

    with vol_app.app_context():
        Session = sessionmaker(bind=db.engine)
        session = Session()

        for l in entry_list:
            try:
                session.add(l)
                session.commit()
            except: 
                print('Unable to add ' + l.ticker + ' to db.')
                continue
        # Commit the changes to the database
        session.close()

    return sorted_data.to_dict('records')


@vol_app.route('/data', methods=['GET'])
def get_vol_data():
    all_data = VolData.query.all()
    all_data_ser = multiple_vol_data_schema.dump(all_data)
    return jsonify(all_data_ser)