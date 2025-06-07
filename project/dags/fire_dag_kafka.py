from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models.param import Param

import psycopg2
import requests
from datetime import datetime
from email.message import EmailMessage
import smtplib


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


def normalize_record_keys(record):
    return {
        'regiao': record.get('REGIAO'),
        'uf': record.get('UF'),
        'estacao': record.get('ESTACAO'),
        'codigo_wmo': record.get('CODIGO (WMO)'),
        'latitude': record.get('LATITUDE'),
        'longitude': record.get('LONGITUDE'),
        'altitude': record.get('ALTITUDE'),
        'data_fundacao': record.get('DATA DE FUNDACAO'),
        'data': record.get('Data'),
        'hora_utc': record.get('Hora_UTC'),
        'precipitacao_total_horario_mm': record.get('PRECIPITACAO_TOTAL_HORARIO_mm'),
        'pressao_atm_estacao_mb': record.get('PRESSAO_ATMOSFERICA_NIVEL_ESTACAO_mB'),
        'pressao_atm_max_hora_ant_mb': record.get('PRESSAO_ATMOSFERICA_MAX_hora_ant_mB'),
        'pressao_atm_min_hora_ant_mb': record.get('PRESSAO_ATMOSFERICA_MIN_hora_ant_mB'),
        'radiacao_global_kj_m2': record.get('RADIACAO_GLOBAL_Kj_m2'),
        'temperatura_ar_bulbo_seco': record.get('TEMPERATURA_DO_AR_BULBO_SECO_C'),
        'temperatura_ponto_orvalho': record.get('TEMPERATURA_DO_PONTO_DE_ORVALHO_C'),
        'temperatura_max_hora_ant': record.get('TEMPERATURA_MAXIMA_hora_ant_C'),
        'temperatura_min_hora_ant': record.get('TEMPERATURA_MINIMA_hora_ant_C'),
        'temperatura_orvalho_max_hora_ant': record.get('TEMPERATURA_ORVALHO_MAX_hora_ant_C'),
        'temperatura_orvalho_min_hora_ant': record.get('TEMPERATURA_ORVALHO_MIN_hora_ant_C'),
        'umidade_rel_max_hora_ant': record.get('UMIDADE_REL_MAX_hora_ant_percent'),
        'umidade_rel_min_hora_ant': record.get('UMIDADE_REL_MIN_hora_ant_percent'),
        'umidade_relativa_horaria': record.get('UMIDADE_RELATIVA_DO_AR_horaria_percent'),
        'vento_direcao_horaria_deg': record.get('VENTO_DIRECAO_horaria_graus'),
        'vento_rajada_maxima_ms': record.get('VENTO_RAJADA_MAXIMA_m_s'),
        'vento_velocidade_horaria_ms': record.get('VENTO_VELOCIDADE_HORARIA_m_s'),
    }


def format_date(date_str):
    for fmt in ('%d/%m/%y', '%d/%m/%Y', '%Y/%m/%d'):
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except Exception:
            continue
    raise ValueError(f"Formato de data desconhecido: {date_str}")


def format_time(hora_utc_str):
    hora_limpa = hora_utc_str.replace(" UTC", "").strip()
    if len(hora_limpa) != 4 or not hora_limpa.isdigit():
        raise ValueError(f"Formato de hora inválido: {hora_utc_str}")
    hora_formatada = f"{hora_limpa[:2]}:{hora_limpa[2:]}"
    try:
        dt = datetime.strptime(hora_formatada, "%H:%M").time()
    except Exception:
        raise ValueError(f"Formato de hora inválido: {hora_utc_str}")
    return dt


def enviar_email(destinatario, assunto, conteudo):
    remetente = 'seu_email@gmail.com'
    senha = 'sua_senha_de_app'  # Use uma senha de aplicativo

    msg = EmailMessage()
    msg.set_content(conteudo)
    msg['Subject'] = assunto
    msg['From'] = remetente
    msg['To'] = destinatario

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(remetente, senha)
        smtp.send_message(msg)


def process_single_record(**context):
    record = context['params'].get('record')
    if record is None:
        raise ValueError(
            "Parâmetro 'record' não encontrado nos params da DAG.")

    record = normalize_record_keys(record)

    if not record['data_fundacao']:
        raise ValueError("Campo 'data_fundacao' está vazio ou inválido.")

    data_fundacao = format_date(record['data_fundacao'])
    data = format_date(record['data'])
    hora_utc = format_time(record['hora_utc'])

    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="192.168.56.1",
        port=5432
    )

    try:
        cur = conn.cursor()

        # 1) Inserir ou recuperar id_estacao na silver_estacao
        cur.execute("""
            SELECT id_estacao FROM silver_estacao
            WHERE regiao = %s AND uf = %s AND estacao = %s AND codigo_wmo = %s
              AND latitude = %s AND longitude = %s AND altitude = %s AND data_fundacao = %s
        """, (
            record['regiao'],
            record['uf'],
            record['estacao'],
            record['codigo_wmo'],
            record['latitude'],
            record['longitude'],
            record['altitude'],
            data_fundacao
        ))
        row = cur.fetchone()
        if row:
            id_estacao = row[0]
        else:
            cur.execute("""
                INSERT INTO silver_estacao (regiao, uf, estacao, codigo_wmo, latitude, longitude, altitude, data_fundacao)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_estacao;
            """, (
                record['regiao'],
                record['uf'],
                record['estacao'],
                record['codigo_wmo'],
                record['latitude'],
                record['longitude'],
                record['altitude'],
                data_fundacao
            ))
            row = cur.fetchone()
            if row:
                id_estacao = row[0]
            else:
                conn.rollback()
                raise ValueError("Falha ao inserir ou recuperar id_estacao.")

        # Sincronizar gold_dim_estacao
        cur.execute(
            "SELECT id_estacao FROM gold_dim_estacao WHERE id_estacao = %s", (id_estacao,))
        if not cur.fetchone():
            cur.execute("""
                SELECT regiao, uf, estacao, codigo_wmo, latitude, longitude, altitude, data_fundacao
                FROM silver_estacao WHERE id_estacao = %s
            """, (id_estacao,))
            estacao_data = cur.fetchone()
            if estacao_data is None:
                conn.rollback()
                raise ValueError(
                    f"Estacao id {id_estacao} não encontrada na silver_estacao para sincronização.")

            cur.execute("""
                INSERT INTO gold_dim_estacao (
                    id_estacao, regiao, uf, estacao, codigo_wmo,
                    latitude, longitude, altitude, data_fundacao
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                id_estacao,
                estacao_data[0], estacao_data[1], estacao_data[2], estacao_data[3],
                estacao_data[4], estacao_data[5], estacao_data[6], estacao_data[7]
            ))

        # 2) Inserir ou recuperar id_tempo
        cur.execute("""
            SELECT id_tempo FROM silver_tempo WHERE data = %s AND hora_utc = %s
        """, (data, hora_utc))
        row = cur.fetchone()
        if row:
            id_tempo = row[0]
        else:
            cur.execute("""
                INSERT INTO silver_tempo (data, hora_utc) VALUES (%s, %s) RETURNING id_tempo;
            """, (data, hora_utc))
            row = cur.fetchone()
            if row:
                id_tempo = row[0]
            else:
                conn.rollback()
                raise ValueError("Falha ao inserir ou recuperar id_tempo.")

        # 3) Inserir ou atualizar silver_meteorologia
        cur.execute("""
            INSERT INTO silver_meteorologia (
                id_estacao, id_tempo, precipitacao_mm, pressao_estacao_mb, pressao_max_mb,
                pressao_min_mb, radiacao_kj_m2, temp_bulbo_seco, temp_ponto_orvalho,
                temp_max, temp_min, temp_orvalho_max, temp_orvalho_min, umidade_max,
                umidade_min, umidade_relativa, vento_direcao_deg, vento_rajada_max_ms,
                vento_velocidade_ms
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (id_estacao, id_tempo) DO UPDATE SET
                precipitacao_mm = EXCLUDED.precipitacao_mm,
                pressao_estacao_mb = EXCLUDED.pressao_estacao_mb,
                pressao_max_mb = EXCLUDED.pressao_max_mb,
                pressao_min_mb = EXCLUDED.pressao_min_mb,
                radiacao_kj_m2 = EXCLUDED.radiacao_kj_m2,
                temp_bulbo_seco = EXCLUDED.temp_bulbo_seco,
                temp_ponto_orvalho = EXCLUDED.temp_ponto_orvalho,
                temp_max = EXCLUDED.temp_max,
                temp_min = EXCLUDED.temp_min,
                temp_orvalho_max = EXCLUDED.temp_orvalho_max,
                temp_orvalho_min = EXCLUDED.temp_orvalho_min,
                umidade_max = EXCLUDED.umidade_max,
                umidade_min = EXCLUDED.umidade_min,
                umidade_relativa = EXCLUDED.umidade_relativa,
                vento_direcao_deg = EXCLUDED.vento_direcao_deg,
                vento_rajada_max_ms = EXCLUDED.vento_rajada_max_ms,
                vento_velocidade_ms = EXCLUDED.vento_velocidade_ms
        """, (
            id_estacao,
            id_tempo,
            record['precipitacao_total_horario_mm'],
            record['pressao_atm_estacao_mb'],
            record['pressao_atm_max_hora_ant_mb'],
            record['pressao_atm_min_hora_ant_mb'],
            record['radiacao_global_kj_m2'],
            record['temperatura_ar_bulbo_seco'],
            record['temperatura_ponto_orvalho'],
            record['temperatura_max_hora_ant'],
            record['temperatura_min_hora_ant'],
            record['temperatura_orvalho_max_hora_ant'],
            record['temperatura_orvalho_min_hora_ant'],
            record['umidade_rel_max_hora_ant'],
            record['umidade_rel_min_hora_ant'],
            record['umidade_relativa_horaria'],
            record['vento_direcao_horaria_deg'],
            record['vento_rajada_maxima_ms'],
            record['vento_velocidade_horaria_ms']
        ))

        # 4) Atualizar gold_fato_meteorologia
        cur.execute("""
            SELECT indice_nesterov FROM gold_fato_meteorologia
            WHERE id_estacao = %s
            ORDER BY id_fato DESC LIMIT 1
        """, (id_estacao,))
        resultado = cur.fetchone()
        acumulado_g_ontem = resultado[0] if resultado else 0

        cur.execute("""
            SELECT calcula_indice_nesterov(%s, %s, %s, %s)
        """, (
            record['temperatura_max_hora_ant'],
            record['umidade_relativa_horaria'],
            record['precipitacao_total_horario_mm'],
            acumulado_g_ontem
        ))
        indice_nesterov_atual = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO gold_fato_meteorologia (
                id_estacao, id_tempo, precipitacao_mm, temperatura_max, temperatura_min,
                umidade_relativa, indice_nesterov, risco_incendio
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                CASE
                    WHEN %s > 4000 THEN 'Perigosíssimo'
                    WHEN %s > 1000 THEN 'Grande'
                    WHEN %s > 500 THEN 'Médio'
                    WHEN %s > 300 THEN 'Fraco'
                    ELSE 'Nenhum'
                END
            )
            RETURNING id_fato;
        """, (
            id_estacao,
            id_tempo,
            record['precipitacao_total_horario_mm'],
            record['temperatura_max_hora_ant'],
            record['temperatura_min_hora_ant'],
            record['umidade_relativa_horaria'],
            indice_nesterov_atual,
            indice_nesterov_atual,
            indice_nesterov_atual,
            indice_nesterov_atual,
            indice_nesterov_atual,
        ))
        id_fato = cur.fetchone()[0]

        # --- Nova etapa: chamar endpoint FastAPI para obter previsão ---
        url_predict = "http://192.168.56.1:8000/predict"  # Ajuste seu URL e porta aqui

        # Dados para enviar no JSON (exemplo, ajuste conforme sua API)
        payload = {
            "id_estacao": id_estacao,
            "id_tempo": id_tempo,
            "indice_nesterov": float(indice_nesterov_atual),
            # Pode passar outras variáveis se necessário...
        }

        response = requests.post(url_predict, json=payload)
        if response.status_code != 200:
            raise ValueError(
                f"Falha ao chamar /predict: {response.status_code} {response.text}")

        # Exemplo: {"1h": "Médio", "6h": "Médio", "12h": "Grande"}
        predicao = response.json()

        # Inserir na tabela gold_predicts
        # Para isso, precisamos das colunas: regiao, uf, estacao, latitude, longitude, altitude, indice_nesterov, risco_incendio_1h, risco_incendio_6h, risco_incendio_12h

        cur.execute("""
            INSERT INTO gold_predicts (
                regiao, uf, estacao, latitude, longitude, altitude, indice_nesterov,
                risco_incendio_1h, risco_incendio_6h, risco_incendio_12h
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (regiao, uf, estacao) DO UPDATE SET
                indice_nesterov = EXCLUDED.indice_nesterov,
                risco_incendio_1h = EXCLUDED.risco_incendio_1h,
                risco_incendio_6h = EXCLUDED.risco_incendio_6h,
                risco_incendio_12h = EXCLUDED.risco_incendio_12h
        """, (
            record['regiao'], record['uf'], record['estacao'],
            record['latitude'], record['longitude'], record['altitude'],
            indice_nesterov_atual,
            predicao.get("risco_incendio_1h"),
            predicao.get("risco_incendio_6h"),
            predicao.get("risco_incendio_12h"),
        ))

        conn.commit()
        cur.close()

        risco_1h = predicao.get("risco_incendio_1h")
        if risco_1h in ["Grande", "Perigosíssimo"]:
            conteudo_email = f"""
            ALERTA DE INCÊNDIO!

            Estação: {record['estacao']}
            Data: {data}
            Hora UTC: {hora_utc}
            Índice de Nesterov: {indice_nesterov_atual}

            Risco de Incêndio em 1h: {risco_1h}

            Predição completa: {predicao}
            """
            enviar_email(
                destinatario='destinatario@gmail.com',
                assunto=f"Alerta de Incêndio: {risco_1h} na estação {record['estacao']}",
                conteudo=conteudo_email
            )

    finally:
        conn.close()

def test():
    print("a")

with DAG(
    dag_id="kafka_dag",
    default_args=default_args,
    start_date=datetime(2023, 5, 10),
    catchup=False,
    tags=["data", "meteorologia", "predict"],
    params={
        "record": Param(
            {}
            # {
            #     "REGIAO": "CO",
            #     "UF": "DF",
            #     "ESTACAO": "BRASILIA",
            #     "CODIGO (WMO)": "A001",
            #     "LATITUDE": -15.78944444,
            #     "LONGITUDE": -47.92583332,
            #     "ALTITUDE": 1160.96,
            #     "DATA DE FUNDACAO": "07/05/00",
            #     "Data": "2025/01/01",
            #     "Hora_UTC": "0000 UTC",
            #     "PRECIPITACAO_TOTAL_HORARIO_mm": 0,
            #     "PRESSAO_ATMOSFERICA_NIVEL_ESTACAO_mB": 886.1,
            #     "PRESSAO_ATMOSFERICA_MAX_hora_ant_mB": 886.1,
            #     "PRESSAO_ATMOSFERICA_MIN_hora_ant_mB": 885.5,
            #     "RADIACAO_GLOBAL_Kj_m2": "null",
            #     "TEMPERATURA_DO_AR_BULBO_SECO_C": 20.8,
            #     "TEMPERATURA_DO_PONTO_DE_ORVALHO_C": 19.5,
            #     "TEMPERATURA_MAXIMA_hora_ant_C": 20.9,
            #     "TEMPERATURA_MINIMA_hora_ant_C": 20.7,
            #     "TEMPERATURA_ORVALHO_MAX_hora_ant_C": 19.5,
            #     "TEMPERATURA_ORVALHO_MIN_hora_ant_C": 19.2,
            #     "UMIDADE_REL_MAX_hora_ant_percent": 92,
            #     "UMIDADE_REL_MIN_hora_ant_percent": 90,
            #     "UMIDADE_RELATIVA_DO_AR_horaria_percent": 92,
            #     "VENTO_DIRECAO_horaria_graus": 8,
            #     "VENTO_RAJADA_MAXIMA_m_s": 3.6,
            #     "VENTO_VELOCIDADE_HORARIA_m_s": 1.8
            # }
        )
    }
) as dag:
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_gold_predicts",
        conn_id="postgres_default",
        sql="""
        CREATE TABLE IF NOT EXISTS gold_predicts (
            regiao TEXT,
            uf CHAR(2),
            estacao TEXT,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            altitude DECIMAL(9,2),
            indice_nesterov DECIMAL(10,4),
            risco_incendio_1h TEXT,
            risco_incendio_6h TEXT,
            risco_incendio_12h TEXT
        );
        """,
    )

    task_insert = PythonOperator(
        task_id="process_single_record_and_predict",
        # python_callable=process_single_record,
        python_callable=test,
    )

    create_bronze >> task_insert
