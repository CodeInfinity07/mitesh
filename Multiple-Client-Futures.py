#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 15:02:49 2021

@author: ryzon
"""

import os
import datetime
import pandas_ta as pta
import btalib
import pandas as pd
import json
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from time import sleep
from binance import ThreadedWebsocketManager
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
import logging
import re
import time
import mysql.connector
import ray

ray.init()
updater = Updater('2028793873:AAFAHKocwFb8aFA5aGVRIAgnL0Tm2ycHhGc', use_context=True)

try:
    connection = mysql.connector.connect(host='localhost',
                                         database='binance',
                                         user='root',
                                         password='zain0980')
except Exception as e:
    print(e)

ts = time.time()
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
testnet_api_key = os.getenv('FUTURES_TESTNET_API_KEY')
testnet_secret_key = os.getenv('FUTURES_TESTNET_API_SECRET')
client = Client(testnet_api_key, testnet_secret_key, testnet=True)
client2 = Client(testnet_api_key, testnet_secret_key)
set_default=True
client.futures_account_balance()[1]['balance']

def get_usdt_balances():
    balance = client.futures_account_balance()[1]['balance']
    return balance


def nospecial(text):
	text = re.sub("[^a-zA-Z0-9]+", " ",text)
	return text

def nospaces(text):
    sentence = text
    pattern = re.compile(r'\b([a-z]) (?=[a-z]\b)', re.I)
    sentence = re.sub(pattern, r'\g<1>', sentence)
    return sentence

def extract_perc(perc, num):
    result = float((perc/100)*num)
    return result

def buy_symbol(symbol, quantity, price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='BUY',
            type='LIMIT',
            quantity=quantity,
            timeInForce='GTC',
            price=price,
            timestamp=ts,
            newOrderRespType='RESULT')

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def sell_symbol(symbol, quantity, price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='SELL',
            type='LIMIT',
            quantity=quantity,
            timeInForce='GTC',
            price=price,
            timestamp=ts,
            newOrderRespType='RESULT')

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def buy_take_profit(symbol, quantity, price, st_price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='BUY',
            type='TAKE_PROFIT',
            quantity=quantity,
            timeInForce='GTE_GTC',
            price=price,
            stopPrice=st_price,
            reduceOnly = 'true',
            newOrderRespType='RESULT',
            timestamp=ts,
            workingType="CONTRACT_PRICE")

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def sell_take_profit(symbol, quantity, price, st_price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='SELL',
            type='TAKE_PROFIT',
            quantity=quantity,
            timeInForce='GTE_GTC',
            price=price,
            stopPrice=st_price,
            reduceOnly = 'true',
            newOrderRespType='RESULT',
            timestamp=ts,
            workingType="CONTRACT_PRICE")
        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def sell_stop_symbol(symbol, quantity, price, st_price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='SELL',
            type='STOP',
            quantity=quantity,
            timeInForce='GTE_GTC',
            price=price,
            stopPrice=st_price,
            timestamp=ts,
            newOrderRespType='RESULT')

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def buy_stop_symbol(symbol, quantity, price, st_price, ts):
    try:
        buy_market = client.futures_create_order(
            symbol=symbol,
            side='BUY',
            type='STOP',
            quantity=quantity,
            timeInForce='GTE_GTC',
            price=price,
            stopPrice=st_price,
            timestamp=ts,
            newOrderRespType='RESULT')

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e


def parser(update, context):
    global client
    global client2
    try:
        temp = update.message.text
    except:
        temp = update.channel_post.text
    target = temp.lower()
    sql_select_Query = "select * from clients"
    cursor = connection.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    for row in records:
        testnet_api_key = row[1]
        testnet_secret_key = row[2]
        client = Client(testnet_api_key, testnet_secret_key, testnet=True)
        client2 = Client(testnet_api_key, testnet_secret_key)
        BUY_PERCENT = row[3]
        SELL_PERCENT = row[4]
        ST_PRICE_PERCENT = row[5]
        DEFAULT_USDT = row[6]
        hatcher.remote(target,BUY_PERCENT,SELL_PERCENT,ST_PRICE_PERCENT,DEFAULT_USDT)
  
@ray.remote
def hatcher(target,BUY_PERCENT,SELL_PERCENT,ST_PRICE_PERCENT,DEFAULT_USDT):
    if "zone" in target:
        pass
    elif "close" in target:
        pass
    elif "short" in target:
        try:
            index_hash = target.lower().index('#')
            token = nospecial(target[index_hash+1:])
        except:
            index_hash = 0
            token = nospecial(target[index_hash])
        if "rebuy" in token.lower():
            index_reb = token.lower().index('rebuy')
            token = token[:index_reb]
        if "buy" in token.lower():
            index_reb = token.lower().index('buy')
            token = token[:index_reb]
        if "spot" in token.lower():
            index_reb = token.lower().index('spot')
            token = token[:index_reb]
        if "setup" in token.lower():
            index_reb = token.lower().index('setup')
            token = token[:index_reb]
        if "scalp" in token.lower():
            index_reb = token.lower().index('scalp')
            token = token[:index_reb]
        token = str(token)+'USDT'
        token = token.replace(" ",'')
        token = token.upper()
        try:
            client.futures_change_leverage(symbol=token, leverage=10)
            quantity_precision = client.futures_orderbook_ticker(symbol=token)
            quantity_precision = quantity_precision['askQty']
            quantity_precision = str(float(quantity_precision))
            qty_precision_index = quantity_precision.index('.')
            qty_precision = len(quantity_precision) - qty_precision_index - 1
            if int(qty_precision) == 1 and int(quantity_precision[-1])==0:
                zero_precision = True
            else:
                zero_precision = False
            temp_precision_value = client2.futures_orderbook_ticker(symbol=token)
            temp_precision_value = float(temp_precision_value['askPrice'])
            symbol_price = temp_precision_value
            non_zero_precision = str(float(temp_precision_value))
            point_precison_index = non_zero_precision.index('.')
            PRECISION_VALUE = len(non_zero_precision) - point_precison_index - 1
            value = float(symbol_price) - extract_perc(BUY_PERCENT, symbol_price)
            symbol_quantity_full = float(DEFAULT_USDT/value)
            symbol_sell_price = round(value, PRECISION_VALUE)
            if PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
                symbol_sell_price = int(symbol_sell_price)
            elif PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==False:
                symbol_sell_price = int(symbol_sell_price)
                symbol_quantity = round(symbol_quantity_full, qty_precision)
            elif zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
            else:
                symbol_quantity = round(symbol_quantity_full, qty_precision)
                symbol_sell_price = round(value, PRECISION_VALUE)

            try:
                resp = sell_symbol(token, symbol_quantity, symbol_sell_price, ts)
                print(resp)
                sell_price = float(resp['avgPrice'])
                second_quantity = float(resp['executedQty'])
                sell_tp_price = float(sell_price - extract_perc(SELL_PERCENT, sell_price))
                sell_stop_price = float(sell_price + extract_perc(ST_PRICE_PERCENT, sell_price))
                symbol_tp_price_ps = round(sell_tp_price, PRECISION_VALUE)
                sell_stop_price_ps = round(sell_stop_price, PRECISION_VALUE)
                resp2 = buy_take_profit(token, second_quantity, symbol_tp_price_ps, symbol_tp_price_ps, ts)
                resp3 = buy_stop_symbol(token, second_quantity, sell_stop_price_ps, sell_stop_price_ps, ts)
                print(resp2)
                print(resp3)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)

    elif "buy" in target or "long" in target:
        #try:
        #    index_hash = target.lower().index('#')
        #    token = nospecial(target[index_hash+1:])
        #except:
        #    index_hash = 0
        #    token = nospecial(target[index_hash])
        if "#" in token.lower():
            index_reb=token.lower().index('#')
            token=token[:index_reb]
        else:
            token_reb = len(token)
        if "rebuy" in token.lower():
            index_reb = token.lower().index('rebuy')
            token = token[:index_reb]
        if "buy" in token.lower():
            index_reb = token.lower().index('buy')
            token = token[:index_reb]
        if "spot" in token.lower():
            index_reb = token.lower().index('spot')
            token = token[:index_reb]
        if "setup" in token.lower():
            index_reb = token.lower().index('setup')
            token = token[:index_reb]
        if "scalp" in token.lower():
            index_reb = token.lower().index('scalp')
            token = token[:index_reb]
        token=token.replace('$','s')
        token = nospecial(token[0])
        token = str(token)+'USDT'
        token = token.replace(" ",'')
        token = token.upper()
        print(token)
        try:
            client.futures_change_leverage(symbol=token, leverage=10)
            quantity_precision = client.futures_orderbook_ticker(symbol=token)
            quantity_precision = quantity_precision['askQty']
            quantity_precision = str(float(quantity_precision))
            qty_precision_index = quantity_precision.index('.')
            qty_precision = len(quantity_precision) - qty_precision_index - 1
            if int(qty_precision) == 1 and int(quantity_precision[-1])==0:
                zero_precision=True
            else:
                zero_precision = False
            temp_precision_value = client2.futures_orderbook_ticker(symbol=token)
            temp_precision_value = float(temp_precision_value['askPrice'])
            symbol_price = temp_precision_value
            non_zero_precision = str(float(temp_precision_value))
            point_precison_index = non_zero_precision.index('.')
            PRECISION_VALUE = len(non_zero_precision) - point_precison_index - 1
            value = symbol_price + float(extract_perc(BUY_PERCENT, symbol_price))
            symbol_quantity_full = float(DEFAULT_USDT/value)
            if PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
                symbol_sell_price = int(symbol_sell_price)
            elif PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==False:
                symbol_sell_price = int(symbol_sell_price)
                symbol_quantity = round(symbol_quantity_full, qty_precision)
            elif zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
            else:
                symbol_quantity = round(symbol_quantity_full, qty_precision)
                symbol_sell_price = round(value, PRECISION_VALUE)
            try:
                resp = buy_symbol(token, symbol_quantity, symbol_sell_price, ts)
                print(resp)
                sell_price = float(resp['avgPrice'])
                second_quantity = float(resp['executedQty'])
                sell_tp_price = float(sell_price + extract_perc(SELL_PERCENT, sell_price))
                sell_stop_price = float(sell_price - extract_perc(ST_PRICE_PERCENT, sell_price))
                symbol_tp_price_ps = round(sell_tp_price, PRECISION_VALUE)
                sell_stop_price_ps = round(sell_stop_price, PRECISION_VALUE)
                resp2 = sell_take_profit(token, second_quantity, symbol_tp_price_ps, symbol_tp_price_ps, ts)
                resp3 = sell_stop_symbol(token, second_quantity, sell_stop_price_ps, sell_stop_price_ps, ts)
                print(resp2)
                print(resp3)
            except Exception as e:
                print(e)

        except Exception as e:
            print(e)


updater.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), parser))
updater.start_polling(timeout=30)
updater.idle()

