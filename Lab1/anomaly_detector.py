from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='latest',  # anomalie tylko w czasie rzeczywistym
    group_id='anomaly-detector-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Słownik przechowujący listy timestampów dla każdego użytkownika
user_history = defaultdict(list)

print("System wykrywania anomalii prędkości uruchomiony...")
print("Monitorowanie: > 3 transakcje w ciągu 60 sekund na użytkownika")

for message in consumer:
    tx = message.value
    user_id = tx['user_id']

    current_time = datetime.fromisoformat(tx['timestamp'])
    
    user_history[user_id].append(current_time)
    
    user_history[user_id] = [
        t for t in user_history[user_id] 
        if (current_time - t).total_seconds() <= 60
    ]
    
    if len(user_history[user_id]) > 3:
        print(f"!!! ALERT ANOMALII !!!")
        print(f"Użytkownik: {user_id} wykonał {len(user_history[user_id])} transakcje w ciągu ostatniej minuty!")
        print(f"Ostatnia transakcja: {tx['tx_id']} o godzinie {current_time.strftime('%H:%M:%S')}")
        print("-" * 30)
