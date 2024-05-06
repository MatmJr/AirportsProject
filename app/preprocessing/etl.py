import requests
import tempfile
from pyspark.sql import SparkSession

def extractFile(file_id, chosen_name):
    """Extracts a file from Google Drive and saves the data into a PySpark DataFrame.
    
    Args:
        file_id (str): The file ID on Google Drive.
        chosen_name (str): The chosen name to assign to the DataFrame.
    
    Returns:
        pyspark.sql.DataFrame: The DataFrame containing the file data.
    """
    spark = SparkSession.builder.appName("Spark-etl").getOrCreate()
    url = f"https://drive.google.com/uc?id={file_id}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an error for unsuccessful responses
        
        # Creating a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
            temp_file.write(response.content)
            temp_file_path = temp_file.name
        
        # Reading the CSV file with PySpark
        df = spark.read.csv(temp_file_path, header=True)
        
        # Showing the data
        df.show()
        
        # Assigning the DataFrame to a variable with the provided name
        globals()[chosen_name] = df
        
        return df
    
    except requests.RequestException as e:
        print(f"Error accessing the file: {e}")
