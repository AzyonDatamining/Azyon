from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator

import csv
import io
import psycopg2
from minio import Minio


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
}


def load_csvs_to_raw():
    client = Minio(
        "192.168.56.1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    bucket_name = "azyon"
    objects = client.list_objects(bucket_name, recursive=True)

    csv_files = [
        obj.object_name for obj in objects
        if obj is not None and obj.object_name is not None and obj.object_name.lower().endswith('.csv')
    ]

    print(f"🔍 Encontrados {len(csv_files)} arquivos CSV.")

    conn = psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="192.168.56.1",
        port=5432
    )
    cur = conn.cursor()

    for object_name in csv_files:
        print(f"➡️ Processando arquivo: {object_name}")

        response = client.get_object(bucket_name, object_name)
        csv_data = response.read().decode('ISO-8859-1')
        response.close()
        response.release_conn()

        f = io.StringIO(csv_data)
        reader = csv.reader(f, delimiter=';')

        try:
            regiao = next(reader)[1].strip()
            uf = next(reader)[1].strip()
            estacao = next(reader)[1].strip()
            codigo_wmo = next(reader)[1].strip()
            latitude = float(next(reader)[1].replace(',', '.').strip())
            longitude = float(next(reader)[1].replace(',', '.').strip())
            altitude = float(next(reader)[1].replace(',', '.').strip())
            data_fundacao = next(reader)[1].strip()

            header = next(reader)  # Pula cabeçalho

            output = io.StringIO()
            writer = csv.writer(output, delimiter='\t',
                                quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

            linhas_preparadas = 0

            for row in reader:
                if not row or len(row) < 2:
                    continue

                row = [x.replace(',', '.').strip() if x else None for x in row]

                full_row = [
                    regiao,
                    uf,
                    estacao,
                    codigo_wmo,
                    latitude,
                    longitude,
                    altitude,
                    data_fundacao
                ] + row[:19]

                if len(full_row) != 27:
                    print(
                        f"⚠️ Linha inválida: esperava 19 dados meteorológicos, recebi {len(row)}: {row}")
                    continue

                # Para COPY, campos None devem ser representados como \N
                writer.writerow(
                    [x if x is not None else r'\N' for x in full_row])
                linhas_preparadas += 1

            if linhas_preparadas == 0:
                print(f"⚠️ Nenhuma linha válida para {object_name}.")
                continue

            output.seek(0)

            copy_sql = """
                COPY bronze_meteorologia (
                    regiao, uf, estacao, codigo_wmo, latitude, longitude, altitude, data_fundacao,
                    data, hora_utc, precipitacao_total_horario_mm, pressao_atm_estacao_mb, pressao_atm_max_hora_ant_mb,
                    pressao_atm_min_hora_ant_mb, radiacao_global_kj_m2, temperatura_ar_bulbo_seco, temperatura_ponto_orvalho,
                    temperatura_max_hora_ant, temperatura_min_hora_ant, temperatura_orvalho_max_hora_ant, temperatura_orvalho_min_hora_ant,
                    umidade_rel_max_hora_ant, umidade_rel_min_hora_ant, umidade_relativa_horaria, vento_direcao_horaria_deg,
                    vento_rajada_maxima_ms, vento_velocidade_horaria_ms
                ) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\\N')
            """

            try:
                cur.copy_expert(sql=copy_sql, file=output)
                conn.commit()
                print(
                    f"✅ {linhas_preparadas} linhas inseridas no banco para {object_name}.")
            except Exception as e:
                conn.rollback()
                print(f"❌ Erro processando {object_name}: {e}")

        except Exception as e:
            print(f"❌ Erro processando {object_name}: {e}")
            continue

    cur.close()
    conn.close()
    print("🏁 Processamento concluído.")

def train_model():
    print("train")


with DAG(
    dag_id='pipeline_meteorologia',
    default_args=default_args,
    description='Pipeline ETL de dados meteorológicos: RAW > Bronze > Silver > Gold',
    catchup=False,
    tags=['meteorologia', 'etl'],
) as dag:

    # 1. Criar tabela Bronze estruturada
    create_bronze = SQLExecuteQueryOperator(
        task_id="create_bronze_meteorologia",
        conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS bronze_meteorologia CASCADE;
        CREATE TABLE bronze_meteorologia (
            regiao TEXT,
            uf CHAR(2),
            estacao TEXT,
            codigo_wmo TEXT,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            altitude DECIMAL(9,2),
            data_fundacao TEXT,
            data TEXT,
            hora_utc TEXT,
            precipitacao_total_horario_mm DECIMAL(5,2),
            pressao_atm_estacao_mb DECIMAL(6,2),
            pressao_atm_max_hora_ant_mb DECIMAL(6,2),
            pressao_atm_min_hora_ant_mb DECIMAL(6,2),
            radiacao_global_kj_m2 DECIMAL(7,2),
            temperatura_ar_bulbo_seco DECIMAL(5,2),
            temperatura_ponto_orvalho DECIMAL(5,2),
            temperatura_max_hora_ant DECIMAL(5,2),
            temperatura_min_hora_ant DECIMAL(5,2),
            temperatura_orvalho_max_hora_ant DECIMAL(5,2),
            temperatura_orvalho_min_hora_ant DECIMAL(5,2),
            umidade_rel_max_hora_ant INT,
            umidade_rel_min_hora_ant INT,
            umidade_relativa_horaria INT,
            vento_direcao_horaria_deg INT,
            vento_rajada_maxima_ms DECIMAL(5,2),
            vento_velocidade_horaria_ms DECIMAL(5,2)
        );
        """,
    )

    # 2. Carregar dados RAW
    load_raw = PythonOperator(
        task_id='load_csv_to_raw',
        python_callable=load_csvs_to_raw
    )

    # 3. Criar tabelas silver e inserir dados
    create_silver_tables = SQLExecuteQueryOperator(
        task_id="create_silver_tables",
        conn_id="postgres_default",
        sql="""
        DROP TABLE IF EXISTS silver_estacao CASCADE;
        CREATE TABLE silver_estacao (
            id_estacao SERIAL PRIMARY KEY,
            regiao TEXT NOT NULL,
            uf CHAR(2) NOT NULL,
            estacao TEXT NOT NULL,
            codigo_wmo TEXT NOT NULL,
            latitude DECIMAL(9,6) NOT NULL,
            longitude DECIMAL(9,6) NOT NULL,
            altitude INT NOT NULL,
            data_fundacao DATE NOT NULL
        );

        DROP TABLE IF EXISTS silver_tempo CASCADE;
        CREATE TABLE silver_tempo (
            id_tempo SERIAL PRIMARY KEY,
            data DATE NOT NULL,
            hora_utc TIME NOT NULL
        );

        DROP TABLE IF EXISTS silver_meteorologia CASCADE;
        CREATE TABLE silver_meteorologia (
            id_meteorologia SERIAL PRIMARY KEY,
            id_estacao INT NOT NULL REFERENCES silver_estacao(id_estacao),
            id_tempo INT NOT NULL REFERENCES silver_tempo(id_tempo),
            precipitacao_mm DECIMAL(5,2),
            pressao_estacao_mb DECIMAL(6,2),
            pressao_max_mb DECIMAL(6,2),
            pressao_min_mb DECIMAL(6,2),
            radiacao_kj_m2 DECIMAL(7,2),
            temp_bulbo_seco DECIMAL(5,2),
            temp_ponto_orvalho DECIMAL(5,2),
            temp_max DECIMAL(5,2),
            temp_min DECIMAL(5,2),
            temp_orvalho_max DECIMAL(5,2),
            temp_orvalho_min DECIMAL(5,2),
            umidade_max INT CHECK (umidade_max BETWEEN 0 AND 100),
            umidade_min INT CHECK (umidade_min BETWEEN 0 AND 100),
            umidade_relativa INT CHECK (umidade_relativa BETWEEN 0 AND 100),
            vento_direcao_deg INT,
            vento_rajada_max_ms DECIMAL(5,2),
            vento_velocidade_ms DECIMAL(5,2)
        );

        INSERT INTO silver_estacao (regiao, uf, estacao, codigo_wmo, latitude, longitude, altitude, data_fundacao)
        SELECT DISTINCT
            regiao,
            uf,
            estacao,
            codigo_wmo,
            latitude,
            longitude,
            altitude,
            TO_DATE(data_fundacao, 'YYYY/MM/DD')
        FROM bronze_meteorologia;

        INSERT INTO silver_tempo (data, hora_utc)
        SELECT DISTINCT
            TO_DATE(data, 'YYYY/MM/DD'),
            hora_utc::time
        FROM bronze_meteorologia;

        INSERT INTO silver_meteorologia (
            id_estacao, id_tempo, precipitacao_mm, pressao_estacao_mb, pressao_max_mb,
            pressao_min_mb, radiacao_kj_m2, temp_bulbo_seco, temp_ponto_orvalho,
            temp_max, temp_min, temp_orvalho_max, temp_orvalho_min, umidade_max,
            umidade_min, umidade_relativa, vento_direcao_deg, vento_rajada_max_ms,
            vento_velocidade_ms
        )
        SELECT
            e.id_estacao,
            t.id_tempo,
            b.precipitacao_total_horario_mm,
            b.pressao_atm_estacao_mb,
            b.pressao_atm_max_hora_ant_mb,
            b.pressao_atm_min_hora_ant_mb,
            b.radiacao_global_kj_m2,
            b.temperatura_ar_bulbo_seco,
            b.temperatura_ponto_orvalho,
            b.temperatura_max_hora_ant,
            b.temperatura_min_hora_ant,
            b.temperatura_orvalho_max_hora_ant,
            b.temperatura_orvalho_min_hora_ant,
            b.umidade_rel_max_hora_ant,
            b.umidade_rel_min_hora_ant,
            b.umidade_relativa_horaria,
            b.vento_direcao_horaria_deg,
            b.vento_rajada_maxima_ms,
            b.vento_velocidade_horaria_ms
        FROM bronze_meteorologia b
        JOIN silver_estacao e ON
            b.regiao = e.regiao
            AND b.uf = e.uf
            AND b.estacao = e.estacao
            AND b.codigo_wmo = e.codigo_wmo
            AND b.latitude = e.latitude
            AND b.longitude = e.longitude
            AND b.altitude = e.altitude
            AND TO_DATE(b.data_fundacao, 'YYYY/MM/DD') = e.data_fundacao
        JOIN silver_tempo t ON
            TO_DATE(b.data, 'YYYY/MM/DD') = t.data
            AND b.hora_utc::time = t.hora_utc;
        """,
    )

    create_gold_and_insert = SQLExecuteQueryOperator(
    task_id="create_gold_and_insert",
    conn_id="postgres_default",
    sql="""
        DROP TABLE IF EXISTS gold_dim_estacao CASCADE;
        CREATE TABLE gold_dim_estacao (
            id_estacao SERIAL PRIMARY KEY,
            uf CHAR(2),
            estacao TEXT,
            codigo_wmo TEXT,
            latitude DECIMAL(9,6),
            longitude DECIMAL(9,6),
            altitude DECIMAL(9,2)
        );

        INSERT INTO gold_dim_estacao (uf, estacao, codigo_wmo, latitude, longitude, altitude)
        SELECT DISTINCT uf, estacao, codigo_wmo, latitude, longitude, altitude FROM silver_estacao;

        DROP TABLE IF EXISTS gold_dim_tempo CASCADE;
        CREATE TABLE gold_dim_tempo (
            id_tempo SERIAL PRIMARY KEY,
            data DATE,
            hora TIME
        );

        INSERT INTO gold_dim_tempo (data, hora)
        SELECT DISTINCT data, hora_utc FROM silver_tempo;

        DROP TABLE IF EXISTS gold_fato_meteorologia CASCADE;
        CREATE TABLE gold_fato_meteorologia (
            id_fato         SERIAL PRIMARY KEY,
            id_estacao      INT REFERENCES gold_dim_estacao(id_estacao),
            id_tempo        INT REFERENCES gold_dim_tempo(id_tempo),
            precipitacao_mm DECIMAL(5,2),
            temperatura_max DECIMAL(5,2),
            temperatura_min DECIMAL(5,2),
            umidade_relativa INT,
            indice_nesterov DECIMAL(10,4),
            risco_incendio TEXT
        );

        -- Função já existente que calcula o índice Nesterov para um registro
        CREATE OR REPLACE FUNCTION calcula_indice_nesterov(
            temp_ar NUMERIC,
            umidade_relativa NUMERIC,
            precipitacao NUMERIC,
            acumulado_g_ontem NUMERIC
        ) RETURNS NUMERIC AS $$
        DECLARE
            es NUMERIC;
            e NUMERIC;
            d NUMERIC;
            g NUMERIC;
            acumulado_g NUMERIC;
            limite_maximo NUMERIC := 10000;  -- limite superior
        BEGIN
            IF temp_ar IS NULL OR umidade_relativa IS NULL OR precipitacao IS NULL OR acumulado_g_ontem IS NULL THEN
                RETURN NULL;
            END IF;

            es := 6.11 * POWER(10, (7.5 * temp_ar) / (237.3 + temp_ar));
            e := es * (umidade_relativa / 100.0);
            d := es - e;
            g := d * temp_ar;

            IF precipitacao <= 2.0 THEN
                acumulado_g := acumulado_g_ontem + g;
            ELSIF precipitacao <= 5.0 THEN
                acumulado_g := 0.5 * acumulado_g_ontem + g;
            ELSIF precipitacao <= 8.0 THEN
                acumulado_g := 0.25 * acumulado_g_ontem + g;
            ELSIF precipitacao <= 10.0 THEN
                acumulado_g := 0.1 * acumulado_g_ontem + g;
            ELSE
                acumulado_g := 0;
            END IF;

            IF acumulado_g > limite_maximo THEN
                acumulado_g := limite_maximo;
            END IF;

            IF acumulado_g < 0 THEN
                acumulado_g := 0;
            END IF;

            RETURN acumulado_g;
        END;
        $$ LANGUAGE plpgsql;

        -- Função para popular a tabela gold_fato_meteorologia com cálculo incremental
        CREATE OR REPLACE FUNCTION popular_gold_fato_meteorologia() RETURNS void AS $$
        DECLARE
            rec RECORD;
            acumulado_g NUMERIC := 0;
            limite_maximo NUMERIC := 10000;
        BEGIN
            DELETE FROM gold_fato_meteorologia;

            FOR rec IN
                SELECT
                    s.id_estacao,
                    s.id_tempo,
                    s.precipitacao_mm,
                    s.temp_max,
                    s.temp_min,
                    s.umidade_relativa,
                    t.data,
                    t.hora
                FROM silver_meteorologia s
                JOIN gold_dim_tempo t ON s.id_tempo = t.id_tempo
                ORDER BY s.id_estacao, t.data, t.hora
            LOOP
                IF rec.temp_max IS NULL OR rec.umidade_relativa IS NULL OR rec.precipitacao_mm IS NULL THEN
                    acumulado_g := 0;
                ELSE
                    acumulado_g := calcula_indice_nesterov(
                        rec.temp_max,
                        rec.umidade_relativa,
                        rec.precipitacao_mm,
                        acumulado_g
                    );
                END IF;

                INSERT INTO gold_fato_meteorologia (
                    id_estacao,
                    id_tempo,
                    precipitacao_mm,
                    temperatura_max,
                    temperatura_min,
                    umidade_relativa,
                    indice_nesterov,
                    risco_incendio
                ) VALUES (
                    rec.id_estacao,
                    rec.id_tempo,
                    rec.precipitacao_mm,
                    rec.temp_max,
                    rec.temp_min,
                    rec.umidade_relativa,
                    acumulado_g,
                    CASE
                        WHEN acumulado_g > 4000 THEN 'Perigosíssimo'
                        WHEN acumulado_g > 1000 THEN 'Grande'
                        WHEN acumulado_g > 500 THEN 'Médio'
                        WHEN acumulado_g > 300 THEN 'Fraco'
                        ELSE 'Nenhum'
                    END
                );
            END LOOP;
        END;
        $$ LANGUAGE plpgsql;

        -- Executa a função que popula a tabela gold_fato_meteorologia
        SELECT popular_gold_fato_meteorologia();
        """,
    )

    # 5. Criar índices
    # create_indices = SQLExecuteQueryOperator(
    #     task_id='create_indices',
    #     conn_id='postgres_default',
    #     sql="""
    #     CREATE INDEX idx_silver_estacao_regiao ON silver_estacao(regiao);
    #     CREATE INDEX idx_silver_estacao_uf ON silver_estacao(uf);
    #     CREATE INDEX idx_silver_estacao_codigo_wmo ON silver_estacao(codigo_wmo);

    #     CREATE INDEX idx_silver_tempo_data ON silver_tempo(data);
    #     CREATE INDEX idx_silver_tempo_hora ON silver_tempo(hora_utc);

    #     CREATE INDEX idx_silver_meteorologia_id_estacao ON silver_meteorologia(id_estacao);
    #     CREATE INDEX idx_silver_meteorologia_id_tempo ON silver_meteorologia(id_tempo);

    #     CREATE INDEX idx_gold_dim_estacao_uf ON gold_dim_estacao(uf);
    #     CREATE INDEX idx_gold_dim_tempo_data ON gold_dim_tempo(data);
    #     CREATE INDEX idx_gold_dim_tempo_hora ON gold_dim_tempo(hora);
    #     CREATE INDEX idx_gold_fato_meteorologia_id_estacao ON gold_fato_meteorologia(id_estacao);
    #     CREATE INDEX idx_gold_fato_meteorologia_id_tempo ON gold_fato_meteorologia(id_tempo);
    #     """
    # )


    train_model_task = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )
    
    # create_bronze >> load_raw >> create_silver_tables >> create_gold_and_insert >> create_indices >> train_model  # type: ignore
    create_bronze >> load_raw >> create_silver_tables >> create_gold_and_insert >> train_model_task  # type: ignore
    # create_bronze >> load_raw >> create_silver_tables >> create_gold_and_insert >> create_indices # type: ignore
