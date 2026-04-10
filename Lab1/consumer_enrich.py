from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    group_id='enrichment-group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Uruchomiono wzbogacanie transakcji o poziom ryzyka...")

for message in consumer:
    tx = message.value
    
    if tx['amount'] > 3000:
        tx['risk_level'] = 'HIGH'
    elif tx['amount'] > 1000:
        tx['risk_level'] = 'MEDIUM'
    else:
        tx['risk_level'] = 'LOW'
    
    print(f"ID: {tx['tx_id']} | Kwota: {tx['amount']:8.2f} | RYZYKO: {tx['risk_level']}")
