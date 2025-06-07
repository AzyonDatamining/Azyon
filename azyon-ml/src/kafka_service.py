from kafka import KafkaConsumer, KafkaProducer
import json
import requests

producer = KafkaProducer(bootstrap_servers='kafka:9092')
consumer = KafkaConsumer('fire-risk-input', bootstrap_servers='kafka:9092')

for message in consumer:
    features = json.loads(message.value)
    response = requests.post("http://api:8000/predict/", json=features)
    prediction = response.json()
    producer.send('fire-risk-output', json.dumps(prediction).encode())

