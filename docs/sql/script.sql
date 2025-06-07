-- DROP e CREATE tabela bronze_meteorologia
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

-- DROP e CREATE tabelas silver
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

-- Inserções Silver
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

-- DROP e CREATE tabelas gold
DROP TABLE IF EXISTS gold_dim_estacao CASCADE;
CREATE TABLE gold_dim_estacao (
    id_estacao SERIAL PRIMARY KEY,
    uf CHAR(2),
    estacao TEXT,
    codigo_wmo TEXT
);

INSERT INTO gold_dim_estacao (uf, estacao, codigo_wmo)
SELECT DISTINCT uf, estacao, codigo_wmo FROM silver_estacao;

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

-- Função que calcula índice Nesterov
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

-- Executa a função para popular gold_fato_meteorologia
SELECT popular_gold_fato_meteorologia();

CREATE INDEX idx_silver_estacao_regiao ON silver_estacao(regiao);
CREATE INDEX idx_silver_estacao_uf ON silver_estacao(uf);
CREATE INDEX idx_silver_estacao_codigo_wmo ON silver_estacao(codigo_wmo);

CREATE INDEX idx_silver_tempo_data ON silver_tempo(data);
CREATE INDEX idx_silver_tempo_hora ON silver_tempo(hora_utc);

CREATE INDEX idx_silver_meteorologia_id_estacao ON silver_meteorologia(id_estacao);
CREATE INDEX idx_silver_meteorologia_id_tempo ON silver_meteorologia(id_tempo);

CREATE INDEX idx_gold_dim_estacao_uf ON gold_dim_estacao(uf);
CREATE INDEX idx_gold_dim_tempo_data ON gold_dim_tempo(data);
CREATE INDEX idx_gold_dim_tempo_hora ON gold_dim_tempo(hora);
CREATE INDEX idx_gold_fato_meteorologia_id_estacao ON gold_fato_meteorologia(id_estacao);
CREATE INDEX idx_gold_fato_meteorologia_id_tempo ON gold_fato_meteorologia(id_tempo);