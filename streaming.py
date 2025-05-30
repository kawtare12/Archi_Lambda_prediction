from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Créer la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
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

# 1. Calcul de la vente totale par produit et ville
total_sales_by_city_product = parsed_stream.groupBy("city", "product_line") \
    .agg({"total": "sum"}) \
    .withColumnRenamed("sum(total)", "total_sales")

# Créer une vue temporaire pour ce calcul
total_sales_by_city_product.createOrReplaceTempView("total_sales_by_city_product_view")

# 2. Filtrage des produits les mieux notés
top_rated_products = parsed_stream.filter(col("rating") >= 4.5)  # Produits avec une note >= 4.5

# Créer une vue temporaire pour les produits les mieux notés
top_rated_products.createOrReplaceTempView("top_rated_products_view")

# 3. Calcul des ventes totales par type de client (Normal, Member)
total_sales_by_customer_type = parsed_stream.groupBy("customer_type") \
    .agg({"total": "sum"}) \
    .withColumnRenamed("sum(total)", "total_sales_by_customer_type")

# Créer une vue temporaire pour ce calcul
total_sales_by_customer_type.createOrReplaceTempView("total_sales_by_customer_type_view")

# 4. Calcul du revenu brut total par produit
total_gross_income_by_product = parsed_stream.groupBy("product_line") \
    .agg({"gross_income": "sum"}) \
    .withColumnRenamed("sum(gross_income)", "total_gross_income")

# Créer une vue temporaire pour ce calcul
total_gross_income_by_product.createOrReplaceTempView("total_gross_income_by_product_view")

# 5. Calcul du total des quantités vendues par produit
total_quantity_by_product = parsed_stream.groupBy("product_line") \
    .agg({"quantity": "sum"}) \
    .withColumnRenamed("sum(quantity)", "total_quantity_sold")

# Créer une vue temporaire pour ce calcul
total_quantity_by_product.createOrReplaceTempView("total_quantity_by_product_view")

# 6. Requêtes pour afficher les résultats filtrés
final_query = spark.sql("""
    SELECT city, product_line, total_sales
    FROM total_sales_by_city_product_view
    
""")

# Afficher les produits les mieux notés
top_rated_query = spark.sql("""
    SELECT id, product_line, rating
    FROM top_rated_products_view
""")

# Afficher les ventes totales par type de client
customer_type_query = spark.sql("""
    SELECT customer_type, total_sales_by_customer_type
    FROM total_sales_by_customer_type_view
""")

# Afficher les revenus bruts totaux par produit
gross_income_query = spark.sql("""
    SELECT product_line, total_gross_income
    FROM total_gross_income_by_product_view
""")

# Afficher la quantité totale vendue par produit
quantity_query = spark.sql("""
    SELECT product_line, total_quantity_sold
    FROM total_quantity_by_product_view
""")

# Écrire les résultats dans la console
query1 = final_query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Utiliser 'append' pour les autres requêtes qui ne sont pas des agrégations continues
query2 = top_rated_query.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query3 = customer_type_query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query4 = gross_income_query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query5 = quantity_query.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
query5.awaitTermination()