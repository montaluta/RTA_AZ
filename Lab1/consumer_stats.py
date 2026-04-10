from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

stats = {}
msg_count = 0

print("Analizuję statystyki kategorii...")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']

    if cat not in stats:
        stats[cat] = {
            'count': 0,
            'total': 0.0,
            'min': amt,
            'max': amt
        }

    s = stats[cat]
    s['count'] += 1
    s['total'] += amt
    s['min'] = min(s['min'], amt)
    s['max'] = max(s['max'], amt)
    
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n=== STATYSTYKI KATEGORII (po {msg_count} tx) ===")
        print(f"{'Kategoria':<15} | {'Liczba':<6} | {'Suma':<10} | {'Min':<8} | {'Max':<8}")
        print("-" * 60)
        
        for c in sorted(stats.keys()):
            v = stats[c]
            print(f"{c:<15} | {v['count']:<6} | {v['total']:>10.2f} | {v['min']:>8.2f} | {v['max']:>8.2f}")
