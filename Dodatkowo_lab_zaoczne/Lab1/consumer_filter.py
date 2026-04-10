from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    # auto_offset_reset='earliest', # to powoduje, ze consumer_filter analizuje tez historyczne dane (zakomentowane, bo dodane dodatkowo)
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    if message.value['amount'] > 1000:
        print(f'!!! ALERT !!! {message.value}')
