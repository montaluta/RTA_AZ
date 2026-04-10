from kafka import KafkaProducer
import json, random, time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

sklepy = ["Warszawa", "Kraków", "Gdańsk", "Wrocław"]
kategorie = ["elektronika", "odzież", "żywność", "książki"]

def generate_transaction():
    tx_id = f"TX{random.randint(1000, 9999)}"
    user_id = f"u{random.randint(1, 20):02d}"
    amount = round(random.uniform(5.0, 5000.0), 2)
    store = random.choice(sklepy)
    category = random.choice(kategorie)
    timestamp = datetime.now().isoformat()
    
    return {
        'tx_id': tx_id,
        'user_id': user_id,
        'amount': amount,
        'store': store,
        'category': category,
        'timestamp': timestamp
    }

i = 0

try:
    while True:
        i += 1
        tx = generate_transaction()
        producer.send('transactions', value=tx)
        print(f"[{i}] {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']}")        
        time.sleep(1)
        
except KeyboardInterrupt:
    print(" Zatrzymywanie producenta...")

finally:
    producer.flush()
    producer.close()
