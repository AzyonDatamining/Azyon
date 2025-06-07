import pandas as pd
import joblib
import mlflow.pyfunc
from predict_utils import preprocess_input
import numpy as np
from mlflow.tracking import MlflowClient

MLFLOW_TRACKING_URI = "http://localhost:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

def carregar_modelo_mais_recente(nome_modelo):
    client = MlflowClient()
    model_metadata = client.get_latest_versions(nome_modelo, stages=["None"])
    latest_model_version = model_metadata[0].version
    model_uri = f"models:/{nome_modelo}/{latest_model_version}"
    modelo = mlflow.pyfunc.load_model(model_uri)
    return modelo

def predict_por_horizonte(nome_modelo, json_input, scaler, historical_data):
    # Pré-processar
    model_input = preprocess_input(json_input, historical_data, scaler)

    # Carregar modelo
    model = carregar_modelo_mais_recente(nome_modelo)

    # Fazer predição
    predictions = model.predict(model_input)

    # Extrair o valor — depende do tipo de retorno
    if isinstance(predictions, pd.DataFrame):
        pred_value = predictions.iloc[0, 0]
    elif isinstance(predictions, pd.Series):
        pred_value = predictions.iloc[0]
    elif isinstance(predictions, (list, tuple, np.ndarray)):
        pred_value = predictions[0]
    else:
        pred_value = predictions  # Caso seja escalar já

    return int(pred_value)

def predict_all_horizons(json_input):
    # 1. Carregar scaler
    scaler = joblib.load('scaler.joblib')

    # 2. Simula histórico da estação
    historical_data = pd.DataFrame([
        {"precipitacao_mm": 0.0, "temperatura_max": 29.5, "temperatura_min": 19.8, "umidade_relativa": 62.0, "indice_nesterov": 110.0},
        {"precipitacao_mm": 0.0, "temperatura_max": 30.1, "temperatura_min": 20.3, "umidade_relativa": 58.0, "indice_nesterov": 105.0},
    ])

    # 3. Fazer predições para cada horizonte
    pred_6h = predict_por_horizonte("fire_risk_model_6", json_input, scaler, historical_data)
    pred_12h = predict_por_horizonte("fire_risk_model_12", json_input, scaler, historical_data)
    pred_24h = predict_por_horizonte("fire_risk_model_1", json_input, scaler, historical_data)

    result = {
        "prediction_6h": pred_6h,
        "prediction_12h": pred_12h,
        "prediction_24h": pred_24h
    }

    return result

# Exemplo de uso:
if __name__ == "__main__":
    input_json = {
        "id_fato": 123,
        "id_estacao": 1,
        "id_tempo": 456,
        "precipitacao_mm": 0.0,
        "temperatura_max": 30.0,
        "temperatura_min": 20.0,
        "umidade_relativa": 60.0,
        "indice_nesterov": 100.0,
        "data": "2025-06-01",
        "hora": "12:00",
        "uf": "SP",
        "estacao": "Campinas",
        "codigo_wmo": 12345
    }

    preds = predict_all_horizons(input_json)
    print(preds)


# from feature_engeneering import preprocess_pipeline

# db_url = "postgresql://airflow:airflow@localhost:5432/airflow"
# df = preprocess_pipeline(db_url)

# features = df.select_dtypes(include=['float64']).columns.tolist()

# print(features)

