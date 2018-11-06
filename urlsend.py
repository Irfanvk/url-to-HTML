#!/usr/bin/env python
import pika
import json
import csv
import urllib3

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='names')

data = []
count= 0
with open('imdb_indian_6464ae86-00fc-47b6-8b05-24d6b47fac77_ImdbItem_1541484904.csv') as file:
    reader = csv.reader(file)

    for url in reader:
        count+=1
        if count <=1 :
            continue
        url = json.dumps(url)
        channel.basic_publish(exchange='',
                      routing_key='names',
                      body=url)
        print(url,"\n")
        print(count)

print(" [x] Sent 'Hello World!'")

connection.close()
