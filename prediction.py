from pyhive import hive
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
import pandas as pd
import time
from pyspark.sql.functions import lit, col

# Hive connection setup
hive_host = 'hive-server'
hive_port = 10000
hive_username = 'hive'
hive_database = 'default'

# Connect to Hive
conn = hive.Connection(host=hive_host, port=hive_port, username=hive_username, database=hive_database)
cursor = conn.cursor()
print("Connected to Hive.")

# Fetch data from Hive sales table
start_time = time.time()
cursor.execute("SELECT * FROM sales")
columns = [col[0] for col in cursor.description]
rows = cursor.fetchall()
sales_data = pd.DataFrame(rows, columns=columns)
print(f"Fetched {len(sales_data)} rows from Hive.")
print(sales_data.columns)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HiveSparkPrediction") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

print("Spark session initialized.")

if sales_data.empty:
    print("No data available in the Hive sales table. Creating an empty output file.")

    # Create an empty DataFrame with the expected output schema
    empty_schema = spark.createDataFrame([], schema="invoice_id STRING, total DOUBLE, prediction DOUBLE")
    output_path = "/tmp/shared_data/predictions_output"
    empty_schema.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "false") \
        .save(output_path)

    print(f"Empty output file saved to {output_path}.")
else:
    # Convert the Hive data to a Spark DataFrame
    sales_df = spark.createDataFrame(sales_data)
    print("Hive data loaded into Spark DataFrame.")
    print(sales_df.columns)
    
    
    # Rename columns to remove the 'sales.' prefix
    for col_name in sales_df.columns:
        if col_name.startswith("sales."):
            sales_df = sales_df.withColumnRenamed(col_name, col_name.split(".")[1])

    print("Renamed columns to remove 'sales.' prefix.")
    print(sales_df.columns)

    # Prepare features for prediction
    assembler = VectorAssembler(
        inputCols=["unit_price", "quantity", "tax_5_percent", "cogs", "gross_income", "rating"],
        outputCol="features"
    )
    sales_with_features = assembler.transform(sales_df)
    print("Features vector assembled.")

    # Load the trained model
    model_path = "/tmp/model"  # Adjust the path to where your model is saved
    model = LinearRegressionModel.load(model_path)
    print("Model loaded successfully.")

    # Perform predictions
    predictions = model.transform(sales_with_features)
    print("Predictions completed.")
    end_time = time.time()
    prediction_time = end_time - start_time  # Temps de prédiction en secondes
    # Ajouter la colonne "temps" (en ajoutant le même temps de prédiction pour toutes les lignes du batch)
    predictions_with_time = predictions.withColumn("temps", lit(prediction_time))
    # Select the required columns
    predictions_output = predictions_with_time.select(col("invoice_id").alias("Invoice ID"), "total", "prediction", "temps")

    # Write the predictions to CSV
    output_path = "/tmp/shared_data/predictions_output"
    predictions_output.write \
        .mode("overwrite") \
        .format("csv") \
        .option("header", "false") \
        .save(output_path)

    print(f"Predictions saved to {output_path}.")