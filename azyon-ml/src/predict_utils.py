import pandas as pd
from feature_engeneering import generate_lag_features, generate_aggregated_features

FEATURES = ['precipitacao_mm', 'temperatura_max', 'temperatura_min', 'umidade_relativa', 'indice_nesterov', 'precipitacao_mm_lag_1', 'temperatura_max_lag_1', 'temperatura_min_lag_1', 'umidade_relativa_lag_1', 'indice_nesterov_lag_1', 'precipitacao_mm_lag_3', 'temperatura_max_lag_3', 'temperatura_min_lag_3', 'umidade_relativa_lag_3', 'indice_nesterov_lag_3', 'precipitacao_mm_lag_6', 'temperatura_max_lag_6', 'temperatura_min_lag_6', 'umidade_relativa_lag_6', 'indice_nesterov_lag_6', 'precipitacao_mm_mean_3h',
            'precipitacao_mm_std_3h', 'temperatura_max_mean_3h', 'temperatura_max_std_3h', 'temperatura_min_mean_3h', 'temperatura_min_std_3h', 'umidade_relativa_mean_3h', 'umidade_relativa_std_3h', 'indice_nesterov_mean_3h', 'indice_nesterov_std_3h', 'precipitacao_mm_mean_6h', 'precipitacao_mm_std_6h', 'temperatura_max_mean_6h', 'temperatura_max_std_6h', 'temperatura_min_mean_6h', 'temperatura_min_std_6h', 'umidade_relativa_mean_6h', 'umidade_relativa_std_6h', 'indice_nesterov_mean_6h', 'indice_nesterov_std_6h']


def preprocess_input(json_input, historical_data, scaler):
    df = pd.concat([historical_data, pd.DataFrame(
        [json_input])], ignore_index=True)
    df = generate_lag_features(df)
    df = generate_aggregated_features(df)

    df_latest = df.iloc[-1:]
    df_latest = df_latest[FEATURES]

    X = df_latest.values
    X_scaled = scaler.transform(X)
    return pd.DataFrame(X_scaled, columns=FEATURES)
