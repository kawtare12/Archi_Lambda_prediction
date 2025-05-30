from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegressionModel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import time  # Pour mesurer le temps

# Créer la session Spark
spark = SparkSession.builder \
    .appName("AppPredictionStreaming") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

# Schéma des données JSON publiées sur Kafka
schema = StructType([
    StructField("id", StringType(), True),
    StructField("invoice_id", StringType(), True),
    StructField("branch", StringType(), True),
    StructField("city", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("product_line", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("tax_5_percent", DoubleType(), True),
    StructField("total", DoubleType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("payment", StringType(), True),
    StructField("cogs", DoubleType(), True),
    StructField("gross_margin_percentage", DoubleType(), True),
    StructField("gross_income", DoubleType(), True),
    StructField("rating", DoubleType(), True)
])
start_time = time.time()
# Lire le flux Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "stream-topic") \
    .load()

# Transformation : Conversion des données JSON
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Préparation des caractéristiques pour la prédiction
assembler = VectorAssembler(
    inputCols=["unit_price", "quantity", "tax_5_percent", "cogs", "gross_income", "rating"], 
    outputCol="features"
)

data_with_features = assembler.transform(parsed_stream)

# Charger le modèle de prédiction
model = LinearRegressionModel.load("/tmp/model")

# Ajouter une colonne "temps" représentant le temps que le modèle prend pour prédire chaque ligne
  # Commencer à mesurer le temps

# Prédictions en streaming
predictions = model.transform(data_with_features)

end_time = time.time()  # Fin de la mesure du temps
prediction_time = end_time - start_time  # Temps de prédiction en secondes

# Ajouter la colonne "temps" (en ajoutant le même temps de prédiction pour toutes les lignes du batch)
predictions_with_time = predictions.withColumn("temps", lit(prediction_time))

# Sélectionner les colonnes nécessaires
predictions_output = predictions_with_time.select(col("invoice_id").alias("Invoice ID"), "total", "prediction", "temps")

# Sauvegarder dans le fichier CSV
output_path = "/tmp/shared_data/predictions_outputlast4"
query = predictions_output.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", output_path) \
    .option("checkpointLocation", "/tmp/shared_data/checkpointlast4") \
    .start()

query.awaitTermination()