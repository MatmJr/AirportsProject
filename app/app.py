from fastapi import FastAPI
from pyspark.ml import PipelineModel

app = FastAPI()

# Carregar o modelo treinado
model_path = "modelling/results/pipeline_model"
loaded_model = PipelineModel.load(model_path)

@app.post("/predict")
async def predict(data: dict):
    # Suponha que 'data' seja um dicionário contendo os dados a serem previstos
    # Realizar pré-processamento, se necessário, nos dados recebidos
    # Fazer a previsão usando o modelo carregado
    prediction = loaded_model.transform(data)
    # Retorna a previsão (por exemplo, como JSON)
    return {"prediction": prediction}
