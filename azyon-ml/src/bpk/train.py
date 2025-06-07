import mlflow
import mlflow.pyfunc
import torch
import torch.nn as nn
import torch.optim as optim
import pandas as pd
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.model_selection import train_test_split
from mlflow.models import evaluate

from model import MultiHorizonClassifier
from feature_engeneering import preprocess_pipeline


class MultiHorizonWrapperT1h(mlflow.pyfunc.PythonModel):
    def __init__(self, model, device):
        self.model = model
        self.device = device

    def predict(self, context, model_input):
        X_tensor = torch.tensor(model_input.values).float().to(self.device)
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(X_tensor)
        preds = torch.argmax(outputs[0], dim=1).cpu().numpy()
        return pd.Series(preds, name="prediction")


class MultiHorizonWrapperT6h(mlflow.pyfunc.PythonModel):
    def __init__(self, model, device):
        self.model = model
        self.device = device

    def predict(self, context, model_input):
        X_tensor = torch.tensor(model_input.values).float().to(self.device)
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(X_tensor)
        preds = torch.argmax(outputs[1], dim=1).cpu().numpy()
        return pd.Series(preds, name="prediction")


class MultiHorizonWrapperT12h(mlflow.pyfunc.PythonModel):
    def __init__(self, model, device):
        self.model = model
        self.device = device

    def predict(self, context, model_input):
        X_tensor = torch.tensor(model_input.values).float().to(self.device)
        self.model.eval()
        with torch.no_grad():
            outputs = self.model(X_tensor)
        preds = torch.argmax(outputs[2], dim=1).cpu().numpy()
        return pd.Series(preds, name="prediction")


def train(db_url):
    mlflow.set_tracking_uri("http://localhost:5000")
    mlflow.set_experiment("fire-risk-classifier")

    df = preprocess_pipeline(db_url)
    print(df.head())

    features = df.select_dtypes(include=['float64']).columns
    X = df[features].values

    le1 = LabelEncoder()
    le6 = LabelEncoder()
    le12 = LabelEncoder()

    y1 = le1.fit_transform(df['risco_incendio_t1h'])
    y6 = le6.fit_transform(df['risco_incendio_t6h'])
    y12 = le12.fit_transform(df['risco_incendio_t12h'])

    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    X_train, X_val, y1_train, y1_val, y6_train, y6_val, y12_train, y12_val = train_test_split(
        X, y1, y6, y12, test_size=0.2, random_state=42
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    model = MultiHorizonClassifier(input_dim=X.shape[1]).to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=0.001)

    X_train_tensor = torch.tensor(X_train).float().to(device)
    y1_train_tensor = torch.tensor(y1_train).long().to(device)
    y6_train_tensor = torch.tensor(y6_train).long().to(device)
    y12_train_tensor = torch.tensor(y12_train).long().to(device)

    with mlflow.start_run() as run:
        epochs = 100
        mlflow.log_param("learning_rate", 0.001)
        mlflow.log_param("epochs", epochs)

        model.train()
        for epoch in range(epochs):
            optimizer.zero_grad()
            outputs = model(X_train_tensor)
            loss = sum([
                criterion(outputs[0], y1_train_tensor),
                criterion(outputs[1], y6_train_tensor),
                criterion(outputs[2], y12_train_tensor)
            ])
            loss.backward()
            optimizer.step()
            mlflow.log_metric("train_loss", loss.item(), step=epoch)

        # Cria DataFrame para validação com nomes das features
        X_val_df = pd.DataFrame(X_val, columns=features)

        # Log modelos PyFunc separados para cada horizonte
        wrapper_t1h = MultiHorizonWrapperT1h(model, device)
        mlflow.pyfunc.log_model(
            artifact_path="fire_risk_model_t1h",
            python_model=wrapper_t1h,
            input_example=X_val_df.head(1)
        )

        wrapper_t6h = MultiHorizonWrapperT6h(model, device)
        mlflow.pyfunc.log_model(
            artifact_path="fire_risk_model_t6h",
            python_model=wrapper_t6h,
            input_example=X_val_df.head(1)
        )

        wrapper_t12h = MultiHorizonWrapperT12h(model, device)
        mlflow.pyfunc.log_model(
            artifact_path="fire_risk_model_t12h",
            python_model=wrapper_t12h,
            input_example=X_val_df.head(1)
        )

        # URIs dos modelos para avaliação
        model_uri_t1h = f"runs:/{run.info.run_id}/fire_risk_model_t1h"
        model_uri_t6h = f"runs:/{run.info.run_id}/fire_risk_model_t6h"
        model_uri_t12h = f"runs:/{run.info.run_id}/fire_risk_model_t12h"

        # Avaliação para cada horizonte - adicionando a coluna target no DataFrame de validação
        for horizon, model_uri, y_val in [
            ("t1h", model_uri_t1h, y1_val),
            ("t6h", model_uri_t6h, y6_val),
            ("t12h", model_uri_t12h, y12_val),
        ]:
            X_val_df_eval = X_val_df.copy()
            X_val_df_eval["target"] = y_val

            result = evaluate(
                model=model_uri,
                data=X_val_df_eval,
                targets="target",
                model_type="classifier",
                evaluators="default"
            )
            print(f"Evaluation {horizon}:", result.metrics)


if __name__ == "__main__":
    db_url = "postgresql://airflow:airflow@localhost:5432/airflow"
    train(db_url)

