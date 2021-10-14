#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 10:05:16 2021

@author: ryzon
"""
import asyncio
import os
import datetime
from binance.client import Client
from binance.exceptions import BinanceAPIException, BinanceOrderException
from binance import ThreadedWebsocketManager
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
import re
import mysql.connector
import logging
import ray
import math 

ray.init()

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)

updater = Updater('2054542295:AAFv8dCBE9U8NWVC5haQDtqHJIGYIZVTV5c', use_context=True)
try:
    connection = mysql.connector.connect(host='166.62.25.253',
                                         database='binance-db',
                                         user='mitesh_jb',
                                         password='MITESHmitesh@2')
except Exception as e:
    print(e)

cursor = connection.cursor()

def truncate(number, decimals):
    """
    Returns a value truncated to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer.")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more.")
    elif decimals == 0:
        return math.trunc(number)

    factor = 10.0 ** decimals
    return math.trunc(number * factor) / factor

def nospecial(text):
	text = re.sub("[^a-zA-Z0-9]+", " ",text)
	return text

def extract_perc(perc, num):
    result = float((perc/100)*num)
    return float(result)

def get_account_balances():
    balance = client.get_account()
    return balance

def get_account_balance(asset):
    ts=client.get_server_time()['serverTime']
    balance = client.get_asset_balance(asset=asset,timestamp=ts)
    return balance['free']


def buy_symbol(symbol, quantity, price):
    ts=client.get_server_time()['serverTime']
    try:
        buy_market = client.create_order(
            symbol=symbol,
            side='BUY',
            type='LIMIT',
            quantity=quantity,
            timeInForce='GTC',
            price=price,
            timestamp=ts,
            newOrderRespType='FULL')

        return buy_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def sell_oco_symbol(symbol, quantity, price, stop_price , stop_limit_price):
    ts=client.get_server_time()['serverTime']
    try:
        sell_market = client.order_oco_sell(
            symbol= symbol,
            quantity= quantity,
            price= price,
            stopPrice= stop_price,
            stopLimitPrice= stop_limit_price,
            stopLimitTimeInForce= 'GTC',
            timestamp=ts)

        return sell_market

    except BinanceAPIException as e:
        return e
    except BinanceOrderException as e:
        return e

def parser(update, context):
    global client
    try:
        temp = update.message.text
    except:
        temp = update.channel_post.text
    target = temp.lower()
    sql_select_Query = "select * from clients"
    connection.reconnect()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    for row in records:
        api_key = str(row[7])
        secret_key = str(row[8])
        client = Client(api_key, secret_key)
        client.API_URL = 'https://api.binance.com/api'
        BUY_PERCENT = float(row[9])
        SELL_PERCENT = float(row[10])
        ST_PRICE_PERCENT = float(row[11])
        STL_PRICE_PERCENT = float(row[12])
        DEFAULT_USDT = float(row[13])
        hatcher.remote(target, BUY_PERCENT,SELL_PERCENT,ST_PRICE_PERCENT,STL_PRICE_PERCENT,DEFAULT_USDT)
    
@ray.remote
def hatcher(target,BUY_PERCENT,SELL_PERCENT,ST_PRICE_PERCENT,STL_PRICE_PERCENT,DEFAULT_USDT):
    if "zone" in target.lower():
        pass
    elif "short" in target.lower():
        pass
    elif "buy" in target.lower():
        #try:
        #    index_hash = target.lower().index('#')
        #    token = nospecial(target[index_hash+1:])
        #except:
        #    index_hash = 0
        #    token = nospecial(target[index_hash])
        token = str(target)
        if "#" in token.lower():
            index_reb=token.lower().index('#')
            token=token[index_reb:]
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
        token = nospecial(token)
        token = str(token)+'USDT'
        token = token.replace(" ",'')
        token = token.upper()
        print(token)
        try:
            exchange_info = client.get_orderbook_ticker(symbol=token)
            div_value = float(exchange_info['askPrice'])
            print('ask price : '+str(div_value))
            symbol_price = div_value
            non_zero_precision = str(float(div_value))
            point_precison_index = non_zero_precision.index('.')
            PRECISION_VALUE = len(non_zero_precision) - point_precison_index - 1
            value = float(symbol_price) + extract_perc(BUY_PERCENT, symbol_price)
            symbol_quantity_full = float(DEFAULT_USDT/value)
            symbol_buy_price = round(value, PRECISION_VALUE)
            quantity_precision = float(exchange_info['askQty'])
            print('ask Quantity : '+str(quantity_precision))
            quantity_precision = str(float(quantity_precision))
            qty_precision_index = quantity_precision.index('.')
            qty_precision = len(quantity_precision) - qty_precision_index - 1
            if int(qty_precision) == 1 and int(quantity_precision[-1])==0:
                zero_precision = True
            else:
                zero_precision = False
            if PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
                symbol_buy_price = int(symbol_buy_price)
            elif PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==False:
                symbol_buy_price = int(symbol_buy_price)
                symbol_quantity = truncate(symbol_quantity_full, qty_precision)
            elif zero_precision==True:
                symbol_quantity = int(symbol_quantity_full)
            else:
                symbol_quantity = truncate(symbol_quantity_full, qty_precision)
                symbol_buy_price = round(value, PRECISION_VALUE)
            try:
                print('buy price : ' + str(symbol_buy_price))
                print('buy quantity : '+str(symbol_quantity))
                print("default usdt : " +str(DEFAULT_USDT))
                resp = buy_symbol(token, symbol_quantity, symbol_buy_price)
                print(resp)
                avg_value = 0
                items = 0
                for key,value in resp.items():
                    if 'price' == key:
                        avg_value = float(avg_value) + float(value)
                        items = float(items) + float(1)
                temp_price = float(avg_value/items)
                total_price = float(temp_price) - extract_perc(ST_PRICE_PERCENT, temp_price)
                #stop_price = round(total_price, PRECISION_VALUE)
                total_limit_price = float(temp_price) - extract_perc(STL_PRICE_PERCENT, temp_price)
                #stop_limit_price = round(total_limit_price, PRECISION_VALUE)
                total_pr = temp_price + extract_perc(SELL_PERCENT, temp_price)
                #price = round(total_pr, PRECISION_VALUE)
                qs=symbol_quantity-extract_perc(0.11,symbol_quantity)
                price=truncate(total_pr,PRECISION_VALUE)
                stop_price=truncate(total_price,PRECISION_VALUE)
                stop_limit_price=truncate(total_limit_price,PRECISION_VALUE)
                if PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==True:
                    symbol_quantity = int(qs)
                    price = int(total_pr)
                    stop_price = int(total_price)
                    stop_limit_price = int(total_limit_price)
                elif PRECISION_VALUE==1 and non_zero_precision[-1]==0 and zero_precision==False:
                    price = int(total_pr)
                    stop_price = int(total_price)
                    stop_limit_price = int(total_limit_price)
                    symbol_quantity = truncate(qs, qty_precision)
                elif zero_precision==True:
                    symbol_quantity = int(qs)
                else:
                    symbol_quantity = truncate(qs, qty_precision)
                    price = truncate(total_pr, PRECISION_VALUE)
                    stop_price = truncate(total_price, PRECISION_VALUE)
                    stop_limit_price = truncate(total_limit_price, PRECISION_VALUE)

                #if zero_precision == True and zero_precision == True:
                #    resp2 = sell_oco_symbol(token, int(symbol_quantity) ,int(price) , int(stop_price), int(stop_limit_price))
                #elif zero_precision == False and zero_precision == True:
                #    resp2 = sell_oco_symbol(token, int(symbol_quantity) ,price , stop_price, stop_limit_price)
                #elif zero_precision == True and zero_precision == False:
                #    resp2 = sell_oco_symbol(token, symbol_quantity ,int(price) , int(stop_price), int(stop_limit_price))
                #else:
                resp2 = sell_oco_symbol(token, symbol_quantity ,price , stop_price, stop_limit_price)
                print(resp2)
            except Exception as e:
                print(e)
        except Exception as e:
            print(e)
                
updater.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), parser))
updater.start_polling(timeout=30, drop_pending_updates=True)
updater.idle()
