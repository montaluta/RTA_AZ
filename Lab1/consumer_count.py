from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    store = message.value['store']
    amount = message.value['amount']

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n--- PODSUMOWANIE (po {msg_count} wiadomościach) ---")
        print(f"{'Sklep':<12} | {'Liczba':<7} | {'Suma':<10} | {'Średnia':<8}")
        print("-" * 50)
        
        for s in sorted(store_counts.keys()):
            count = store_counts[s]
            total = total_amount[s]
            avg = total / count if count > 0 else 0
            print(f"{s:<12} | {count:<7} | {total:>10.2f} | {avg:>8.2f}")
        print("-" * 50)
