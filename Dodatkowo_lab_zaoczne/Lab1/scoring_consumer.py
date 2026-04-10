from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer('transactions', bootstrap_servers='broker:9092',
    auto_offset_reset='earliest', group_id='scoring-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

alert_producer = KafkaProducer(bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# TWÓJ KOD
# Dla każdej transakcji: scoruj, jeśli >= 3: wyślij do 'alerts' i wypisz ALERT

def score_transaction(tx):
    score = 0
    rules = []
    
   # R1: kwota > 3000 (+3 pkt)
    if tx['amount'] > 3000:
        score += 3
        rules.append('R1')
        
    # R2: elektronika i kwota > 1500
    if tx['category'] == 'elektronika' and tx['amount'] > 1500:
        score += 2
        rules.append('R2')
        
    # R3: godzina nocna < 6
    hour = int(tx['timestamp'][11:13])
    
    if hour < 6:
        score += 2
        rules.append('R3')
        
    return score, rules

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

alert_producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for message in consumer:
    tx = message.value
    
    score, rules = score_transaction(tx)
    
    if score >= 3:
        tx['score'] = score
        tx['rules'] = rules
        
        alert_producer.send('alerts', value=tx)
        
        print(f"!!! ALERT !!! ID: {tx['tx_id']} | Score: {score} | Rules: {rules}")

alert_producer.flush()
