#!/usr/bin/env python

import sys
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
import streamlit as st
from confluent_kafka import Consumer, OFFSET_BEGINNING
import pandas as pd
import matplotlib.pyplot as plt

def parse_stock_data(value):
    """Parse stock data from the consumed message value."""
    print("VALUE " , value)
    data_parts = value.split(',')
    print(data_parts)
    if data_parts:
        data = {part.split('=')[0]: float(part.split('=')[1]) for part in data_parts if '=' in part}
        return data

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    parser.add_argument('ticker', nargs='+', default=['QQQ'])  # Allow multiple tickers
    parser.add_argument('--reset', action='store_true')
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])
    config.update(config_parser['consumer'])

    ticker = args.ticker
    print('ticker', ticker)

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    # topic = ticker
    consumer.subscribe(args.ticker, on_assign=reset_offset)  # Subscribe to multiple tickers
    # data = []  # List to hold incoming data for plotting
    # print('topic', topic)

    # DataFrame to store the fetched data
    columns = ['Ticker', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume']
    data = pd.DataFrame(columns=columns)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print(f"ERROR: {msg.error()}")
            else:
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                stock_data = parse_stock_data(key)
                print(f"Consumed event from topic {msg.topic()}: key = {value} value = {key}")
                # Append data to DataFrame
                if stock_data: 
                    new_row_df = pd.DataFrame([{
                        'Ticker': msg.topic(),
                        'Time': key,
                        'Open': stock_data['open'],
                        'High': stock_data['high'],
                        'Low': stock_data['low'],
                        'Close': stock_data['close'],
                        # 'Volume': stock_data['volume']
                    }])

                    data = pd.concat([data, new_row_df], ignore_index=True)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

    plt.figure(figsize=(10, 8))

    for ticker in set(data['Ticker']):
        ticker_data = data[data['Ticker'] == ticker]
    
        # Plotting the closing prices for each ticker
        plt.plot(ticker_data['Time'], ticker_data['Close'], label='Close')
        plt.title(f"Closing Prices for {ticker}")
        plt.xlabel("Time")
        plt.ylabel("Price")
        plt.xticks(rotation=90)
        plt.legend()
        try:
            plt.tight_layout()
        except UserWarning:
            print(f"Warning: Tight layout not applied for {ticker}")
        plt.savefig(f'{ticker}_closing_prices.png')