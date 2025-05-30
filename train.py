from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Initialiser SparkSession
spark = SparkSession.builder.appName("appTrain").getOrCreate()

# Charger les données
data_path = "/data/sales.csv"  # Nouveau chemin du fichier CSV dans le conteneur
data = spark.read.csv(data_path, header=True, inferSchema=True)

# Afficher les premières lignes pour vérification
data.show(5)
all_columns = [
    "Invoice ID", "Branch", "City", "Customer type", "Gender", "Product line",
    "Unit price", "Quantity", "Tax 5%", "Total", "Date", "Time", "Payment",
    "cogs", "gross margin percentage", "gross income", "Rating"
]

# Colonnes utilisées pour les caractéristiques et la cible
features = ['Unit price', 'Quantity', 'Tax 5%', 'cogs', 'gross income', 'Rating']
target = 'Total'

# Assembler les caractéristiques en un vecteur unique
assembler = VectorAssembler(inputCols=features, outputCol="features")
data = assembler.transform(data)

# Diviser les données en ensembles d'entraînement et de test
train_data, test_data = data.randomSplit([0.8, 0.2], seed=42)

# Créer et entraîner un modèle de régression linéaire
lr = LinearRegression(featuresCol="features", labelCol=target)
model = lr.fit(train_data)

# Faire des prédictions sur l'ensemble de test
predictions = model.transform(test_data)

# Évaluer le modèle
evaluator = RegressionEvaluator(labelCol=target, predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE): {rmse}")

# Afficher la différence entre les valeurs réelles et prédites pour les premières lignes
predictions.select("Invoice ID", target, "prediction").show(5)

# Sauvegarder le modèle
model_save_path = "/tmp/model"  # Utilisez le chemin complet du répertoire dans /tmp
model.write().overwrite().save(model_save_path)
print(f"Modèle sauvegardé sous : {model_save_path}")


# Arrêter la session Spark
spark.stop()