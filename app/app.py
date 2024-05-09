from preprocessing.etl import ETLProcessor
from modelling.model import LogisticRegressionTrainer

file_id1 = "1s5WHBz59d4v8I96CSRlqR9DWTsSU6HC_"
file_id2 = "1bJ8Nguso8mQPeAE4b_tUg6N_fwH8VmnN"
file_id3 = "1nWR3JWNCTByX725WU2nUaibDX70QBhpu"

# Usage:
etl_processor = ETLProcessor()

# Extract files
airports = etl_processor.extract_file(file_id1, 'airports')
flights = etl_processor.extract_file(file_id2, 'flights')
planes = etl_processor.extract_file(file_id3, 'planes')

# Preprocess data
model_data = etl_processor.preprocess_data(airports, flights, planes)

# Save transformed data
model_data.write.csv('data/model_data.csv', header=True, mode='overwrite')

# Create pipeline and transform data
piped_data = etl_processor.create_pipeline(model_data)

# Uso da classe LogisticRegressionTrainer
trainer = LogisticRegressionTrainer(piped_data)
trainer.train()
trainer.evaluate()