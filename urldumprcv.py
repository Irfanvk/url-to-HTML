#!/usr/bin/env python
import pika
import pymongo
import uuid
import requests
from pymongo import MongoClient

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

client = MongoClient('mongodb://localhost:27017/')

db = client['imdb_']

collection = db['imdb_collection2']

channel.queue_declare(queue='names')


def callback(ch, method, properties, body):
    bdy = body.decode()
    bdy = bdy.strip('[]"')
    print(" [x] Received %r" % bdy)
    url = bdy
    new_name = uuid.uuid5(uuid.NAMESPACE_URL,url)

    HEADERS = {
   'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
   'accept-encoding': 'gzip, deflate, br',
   'accept-language': 'en-GB,en-US;q=0.8,en;q=0.6',
   'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36',
}

    r = requests.get(url,headers=HEADERS)
    response = r.content
    f = open('/home/user/mysys/jb6/dumps/%s.html' %new_name, 'w')
    f.write(str(response))
    f.close
    db.collection.insert({'url':url})

channel.basic_consume(callback,
                      queue='names',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()