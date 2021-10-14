#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 26 10:04:40 2021

@author: ryzon
"""

import socket
import os
from _thread import *
from telegram import Update
from telegram.ext import Updater, CommandHandler, CallbackContext, MessageHandler, Filters
import threading

clients = set()
clients_lock = threading.Lock()

ServerSideSocket = socket.socket()
host = '127.0.0.1'
port = 2022
ThreadCount = 0
updater = Updater('2028793873:AAFAHKocwFb8aFA5aGVRIAgnL0Tm2ycHhGc', use_context=True)

try:
    ServerSideSocket.bind((host, port))
except socket.error as e:
    print(str(e))

print('Socket is listening..')
ServerSideSocket.listen(5)

def multi_threaded_client(connection):
    connection.send(str.encode('Server is working:'))
    def parser(update, context):
            connection.sendall(str.encode(update.message.text))
    while True:
        updater.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), parser))
    connection.close()
    
def parser(update, context):
    for c in clients:
        try:
            c.sendall(str.encode(update.message.text))
        except:
            c.sendall(str.encode(update.channel_post.text))
            
while True:
    Client, address = ServerSideSocket.accept()
    clients.add(Client)
    updater.dispatcher.add_handler(MessageHandler(Filters.text & (~Filters.command), parser))
    print('Connected to: ' + address[0] + ':' + str(address[1]))
    start_new_thread(multi_threaded_client, (Client, ))
    ThreadCount += 1
    print('Thread Number: ' + str(ThreadCount))
    updater.start_polling(timeout=30)
ServerSideSocket.close()
