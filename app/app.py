from preprocessing.etl import extractFile
from pyspark.ml.feature import StringIndexer, OneHotEncoder
import pandas as pd
from pyspark.ml.feature import  VectorAssembler
from pyspark.ml import Pipeline


file_id = '1s5WHBz59d4v8I96CSRlqR9DWTsSU6HC_'
file_id2 = "1bJ8Nguso8mQPeAE4b_tUg6N_fwH8VmnN"
file_id3 = "1nWR3JWNCTByX725WU2nUaibDX70QBhpu"

airports = extractFile(file_id, 'airports')
flights = extractFile(file_id2, 'flights')
planes = extractFile(file_id3, "planes")

# Rename the faa column
airports = airports.withColumnRenamed('faa','dest')
# Rename year column on panes to avoid duplicate column name
planes = planes.withColumnRenamed('year', 'plane_year')

# Join the DataFrames using the "origin" column from the `flights` DataFrame and the "faa" column from the `airports` DataFrame
airports_flights = flights.join(airports, on="dest", how='leftouter')

#join the flights and plane table use key as tailnum column
model_data = airports_flights.join(planes, on='tailnum', how='leftouter')

# Save PySpark DataFrame as CSV
model_data.write.csv('data/model_data.csv', header=True, mode='overwrite')

model_data = model_data.withColumn('arr_delay', model_data.arr_delay.cast('integer'))
model_data = model_data.withColumn('air_time' , model_data.air_time.cast('integer'))
model_data = model_data.withColumn('month', model_data.month.cast('integer'))
model_data = model_data.withColumn('plane_year', model_data.plane_year.cast('integer'))

# Create a new column
model_data =model_data.withColumn('plane_age', model_data.year - model_data.plane_year)

model_data = model_data.withColumn('is_late', model_data.arr_delay >0)

model_data = model_data.withColumn('label', model_data.is_late.cast('integer'))

model_data.filter("arr_delay is not NULL and dep_delay is not NULL and air_time is not NULL and plane_year is not NULL")

#Create a StringIndexer
carr_indexer = StringIndexer(inputCol='carrier', outputCol='carrier_index')
#Create a OneHotEncoder
carr_encoder = OneHotEncoder(inputCol='carrier_index', outputCol='carr_fact')
# encode the dest column just like you did above
dest_indexer = StringIndexer(inputCol='dest', outputCol='dest_index')
dest_encoder = OneHotEncoder(inputCol='dest_index', outputCol='dest_fact')

# Assemble a  Vector
vec_assembler =VectorAssembler(inputCols=['month', 'air_time','carr_fact','dest_fact','plane_age'],
                              outputCol='features',handleInvalid="skip")

# #### Create the pipeline
# Pipeline is a class in the `pyspark.ml module` that combines all the Estimators and Transformers that you've 
# already created.

flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])

piped_data =flights_pipe.fit(model_data).transform(model_data)



