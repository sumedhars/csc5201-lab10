#!/usr/bin/env python

import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

import requests
import numpy as np
import pandas as pd
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import yfinance as yf

def get_latest_stock_price(stock):
    data = stock.history()
    latest_stock_price = data['Close'].iloc[-1]
    return latest_stock_price

def get_today():
    return date.today()

def get_next_day_str(today):
    return get_date_from_string(today) + timedelta(days=1)

def get_date_from_string(expiration_date):
    return datetime.strptime(expiration_date, "%Y-%m-%d").date()

def get_string_from_date(expiration_date):
    return expiration_date.strftime('%Y-%m-%d')


if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('ticker', nargs='+', default=['QQQ'])  # Allow multiple tickers
    parser.add_argument('--mode', choices=['real-time', 'development'], default='development')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # read the ticker
    # ticker = args.ticker
    # print('ticker', ticker)

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            
    count = 0
    # Produce data by repeatedly fetching today's stock prices - feel free to change

    for ticker in args.ticker:  # iterate over multiple tickers

        if args.mode == 'real-time':
            # Real-time mode: Fetch current stock quote
            stock = yf.Ticker(ticker)
            data = stock.history(period="1d")  # fetches the last day data
            # make sure there is data
            if not data.empty:
                latest_data = data.iloc[-1]  # Get the most recent data point
                value = f"open={latest_data['Open']},high={latest_data['High']},low={latest_data['Low']},close={latest_data['Close']},volume={latest_data['Volume']}"
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                producer.produce(topic, key=now, value=value, callback=delivery_callback)
                producer.poll(0)
            else:
                print(f"No data available for {ticker}")

        else:
            # Development mode: Fetch historical data for a fixed date range
            sd = get_date_from_string('2024-04-01')
            ed = sd + timedelta(days=1)
            dfvp = yf.download(tickers=ticker, start=sd, end=ed, interval="1m")

            topic = ticker
            for index, row in dfvp.iterrows():
                # print(row)
                # print(index, row['Open'], row['High'], row['Low'], row['Close'], row['Volume'])  # Debug only
                value = "open=" + str(row['Open']) + ",high=" + str(row['High']) + ",low=" + str(row['Low']) + ",close=" + str(row['Close']) + ",volume=" + str(row['Volume'])
                producer.produce(topic, str(index), value, callback=delivery_callback)
                count += 1
                time.sleep(1)  

        producer.poll(10000)
        producer.flush()
