import pandas as pd
from sqlalchemy import create_engine

def extract_data(db_url):
    engine = create_engine(db_url)
    query = """
    SELECT 
        f.id_fato, f.id_estacao, f.id_tempo, f.precipitacao_mm, 
        f.temperatura_max, f.temperatura_min, f.umidade_relativa, 
        f.indice_nesterov, f.risco_incendio,
        t.data, t.hora,
        e.uf, e.estacao, e.codigo_wmo
    FROM gold_fato_meteorologia f
    JOIN gold_dim_tempo t ON f.id_tempo = t.id_tempo
    JOIN gold_dim_estacao e ON f.id_estacao = e.id_estacao
    ORDER BY e.codigo_wmo, t.data, t.hora
    """
    df = pd.read_sql(query, engine)
    return df

def generate_lag_features(df, lags=[1, 3, 6]):
    for lag in lags:
        for col in ['precipitacao_mm', 'temperatura_max', 'temperatura_min', 'umidade_relativa', 'indice_nesterov']:
            df[f'{col}_lag_{lag}'] = df.groupby('codigo_wmo')[col].shift(lag)
    return df

def generate_aggregated_features(df, windows=[3, 6]):
    for window in windows:
        for col in ['precipitacao_mm', 'temperatura_max', 'temperatura_min', 'umidade_relativa', 'indice_nesterov']:
            df[f'{col}_mean_{window}h'] = df.groupby('codigo_wmo')[col].rolling(window).mean().reset_index(0, drop=True)
            df[f'{col}_std_{window}h'] = df.groupby('codigo_wmo')[col].rolling(window).std().reset_index(0, drop=True)
    return df

def generate_targets(df):
    for h in [1, 6, 12]:
        df[f'risco_incendio_t{h}h'] = df.groupby('codigo_wmo')['risco_incendio'].shift(-h)
    return df

def preprocess_pipeline(db_url):
    df = extract_data(db_url)
    df = generate_lag_features(df)
    df = generate_aggregated_features(df)
    df = generate_targets(df)
    df = df.dropna()
    return df
