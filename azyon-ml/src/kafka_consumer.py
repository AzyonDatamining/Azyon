from kafka import KafkaConsumer
import json
import requests
from datetime import datetime, timezone

AIRFLOW_BASE_URL = "http://192.168.1.10:8080"
DAG_ID = "kafka_dag"
AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "r3RPY3s7Av952uVw"

def get_jwt_token():
    url = f"{AIRFLOW_BASE_URL}/auth/token"
    payload = {
        "username": AIRFLOW_USERNAME,
        "password": AIRFLOW_PASSWORD
    }
    headers = {'Content-Type': 'application/json'}

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code in [200, 201]:
        token = response.json().get("access_token")
        if token:
            return token
        else:
            print("Token não encontrado na resposta")
            return None
    else:
        print(f"Falha ao obter token: {response.status_code} {response.text}")
        return None

def trigger_dag(dag_id, conf=None):
    token = get_jwt_token()
    if not token:
        print("Não foi possível obter token, abortando trigger DAG")
        return

    url = f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns"
    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat()
    }
    if conf:
        payload['conf'] = conf

    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.post(url, json=payload, headers=headers)

    if response.status_code in [200, 201]:
        print(f"DAG {dag_id} disparada com sucesso!")
    else:
        print(f"Erro ao disparar DAG: {response.status_code} {response.text}")

def main():
    consumer = KafkaConsumer(
        'fila_dados_meteorologicos',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='grupo_dados_meteorologicos',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Consumidor Kafka aguardando mensagens...")

    for message in consumer:
        dados = message.value
        print("Mensagem recebida, disparando DAG com dados:")
        print(json.dumps(dados, indent=2))

        trigger_dag(DAG_ID, conf=dados)

if __name__ == "__main__":
    main()
