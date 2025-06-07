from fastapi import FastAPI, HTTPException, APIRouter
from pydantic import BaseModel, Field
from typing import Optional, Dict
from kafka import KafkaProducer
import json
from infer import predict_all_horizons
from send_email import send_severity_email

# Configurações
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPICO_KAFKA = "fila_dados_meteorologicos"

# Inicializa Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Instancia a aplicação
app = FastAPI(title="API de Predição de Risco de Incêndio", version="1.0.0")

# Cria router com prefixo
api_router = APIRouter(prefix="/api/v1", tags=["Previsão de Incêndios"])


# Modelos Pydantic
class DadosMeteorologicos(BaseModel):
    REGIAO: str
    UF: str
    ESTACAO: str
    CODIGO_WMO: str = Field(..., alias="CODIGO (WMO)")
    LATITUDE: float
    LONGITUDE: float
    ALTITUDE: float
    DATA_DE_FUNDACAO: str = Field(..., alias="DATA DE FUNDACAO")
    Data: str
    Hora_UTC: str
    PRECIPITACAO_TOTAL_HORARIO_mm: float
    PRESSAO_ATMOSFERICA_NIVEL_ESTACAO_mB: float
    PRESSAO_ATMOSFERICA_MAX_hora_ant_mB: float
    PRESSAO_ATMOSFERICA_MIN_hora_ant_mB: float
    RADIACAO_GLOBAL_Kj_m2: Optional[float]
    TEMPERATURA_DO_AR_BULBO_SECO_C: float
    TEMPERATURA_DO_PONTO_DE_ORVALHO_C: float
    TEMPERATURA_MAXIMA_hora_ant_C: float
    TEMPERATURA_MINIMA_hora_ant_C: float
    TEMPERATURA_ORVALHO_MAX_hora_ant_C: float
    TEMPERATURA_ORVALHO_MIN_hora_ant_C: float
    UMIDADE_REL_MAX_hora_ant_percent: int
    UMIDADE_REL_MIN_hora_ant_percent: int
    UMIDADE_RELATIVA_DO_AR_horaria_percent: int
    VENTO_DIRECAO_horaria_graus: int
    VENTO_RAJADA_MAXIMA_m_s: float
    VENTO_VELOCIDADE_HORARIA_m_s: float


class EmailData(BaseModel):
    send_to: str
    severity: str
    uf: str
    station: str


# Endpoints
@api_router.post("/new_data", response_model=Dict[str, str])
async def new_data(data: DadosMeteorologicos):
    """
    Envia dados meteorológicos para o Kafka.
    """
    try:
        producer.send(TOPICO_KAFKA, data.dict(by_alias=True))
        producer.flush()
        return {"message": "Dados enviados para o Kafka com sucesso"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao enviar para Kafka: {str(e)}"
        )


@api_router.post("/predict", response_model=Dict[str, int])
async def predict(data: DadosMeteorologicos):
    """
    Realiza predição de risco de incêndio.
    """
    try:
        result = predict_all_horizons(data)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao fazer a predição: {str(e)}"
        )


@api_router.post("/send-mail", response_model=Dict[str, str])
async def send_mail(data: EmailData):
    """
    Envia e-mail com a severidade do risco.
    """
    try:
        send_severity_email(data)
        return {"message": "E-mail enviado com sucesso"}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Erro ao enviar e-mail: {str(e)}"
        )


# Inclui router na app
app.include_router(api_router)

# Rodar com: uvicorn api:app --reload
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8000, reload=True)
