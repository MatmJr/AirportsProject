import requests
import tempfile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline

class ETLProcessor:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Spark-ETL").getOrCreate()

    def extract_file(self, file_id, chosen_name):
        """Extracts a file from Google Drive and saves the data into a PySpark DataFrame."""
        url = f"https://drive.google.com/uc?id={file_id}"
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raises an error for unsuccessful responses

            # Creating a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                temp_file.write(response.content)
                temp_file_path = temp_file.name

            # Reading the CSV file with PySpark
            df = self.spark.read.csv(temp_file_path, header=True)

            # Showing the data
            df.show()

            # Assigning the DataFrame to a variable with the provided name
            setattr(self, chosen_name, df)

            return df

        except requests.RequestException as e:
            print(f"Error accessing the file: {e}")

    def preprocess_data(self, airports, flights, planes):
        """Preprocesses the data by joining, renaming columns, and performing necessary transformations."""
        airports = airports.withColumnRenamed('faa', 'dest')
        planes = planes.withColumnRenamed('year', 'plane_year')

        airports_flights = flights.join(airports, on="dest", how='leftouter')
        model_data = airports_flights.join(planes, on='tailnum', how='leftouter')

        model_data = model_data.withColumn('arr_delay', col('arr_delay').cast('integer'))
        model_data = model_data.withColumn('air_time', col('air_time').cast('integer'))
        model_data = model_data.withColumn('month', col('month').cast('integer'))
        model_data = model_data.withColumn('plane_year', col('plane_year').cast('integer'))

        model_data = model_data.withColumn('plane_age', model_data.year - model_data.plane_year)
        model_data = model_data.withColumn('is_late', model_data.arr_delay > 0)
        model_data = model_data.withColumn('label', model_data.is_late.cast('integer'))

        return model_data

    def create_pipeline(self, model_data):
        """Creates a feature transformation pipeline."""
        dest_indexer = StringIndexer(inputCol='dest', outputCol='dest_index')
        dest_encoder = OneHotEncoder(inputCol='dest_index', outputCol='dest_fact')

        carr_indexer = StringIndexer(inputCol='carrier', outputCol='carrier_index')
        carr_encoder = OneHotEncoder(inputCol='carrier_index', outputCol='carr_fact')

        vec_assembler = VectorAssembler(inputCols=['month', 'air_time', 'carr_fact', 'dest_fact', 'plane_age'],
                                        outputCol='features', handleInvalid="skip")

        flights_pipe = Pipeline(stages=[dest_indexer, dest_encoder, carr_indexer, carr_encoder, vec_assembler])

        return flights_pipe.fit(model_data).transform(model_data)

