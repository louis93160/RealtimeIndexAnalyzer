from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, min, max, avg, expr
import yaml
from kafka_consumer import create_consumer
from postgresql_writer import insert_into_postgresql

def load_config(config_file):
    # Charger la configuration YAML depuis un fichier
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def create_spark_session(app_name):
    # Initialiser une session Spark
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def read_kafka_stream(spark, kafka_config):
    # Lire le flux Kafka avec les configurations données
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
        .option("subscribe", kafka_config['topic']) \
        .load()

def parse_kafka_message(df):
    # Parser les messages JSON du flux Kafka
    df = df.selectExpr("CAST(value AS STRING)")
    return df.selectExpr("from_json(value, 'Adj Close DOUBLE, Close DOUBLE, Datetime STRING, Symbol STRING, Volume DOUBLE') as data").select("data.*")

def aggregate_data(df, window_duration):
    # Agréger les données sur une fenêtre temporelle spécifiée
    return df.groupBy(
        col("Symbol"),
        window(col("Datetime"), window_duration)
    ).agg(
        min(col("Close")).alias(f"low_{window_duration}"),
        max(col("Close")).alias(f"high_{window_duration}"),
        avg(col("Close")).alias(f"avg_{window_duration}"),
        (max(col("Close")) - min(col("Close"))).alias(f"diff_{window_duration}"),
        expr(f"(max(Close) - min(Close)) / min(Close)").alias(f"rate_{window_duration}")
    )

def write_to_postgresql(batch_df, batch_id):
    # Écrire les données agrégées dans PostgreSQL
    batch_df.foreachPartition(insert_into_postgresql)

def main():
    # Charger la configuration
    config = load_config('config/config.yaml')

    # Créer la session Spark
    spark = create_spark_session("StockDataAggregator")
    
    # Lire le flux Kafka
    kafka_df = read_kafka_stream(spark, config['kafka'])
    
    # Parser les messages Kafka
    stock_df = parse_kafka_message(kafka_df)
    
    # Agréger les données sur une fenêtre de 1 minute
    agg_1m_df = aggregate_data(stock_df, "1 minute")
    
    # Agréger les données sur une fenêtre de 5 minutes
    agg_5m_df = aggregate_data(stock_df, "5 minutes")

    # Fusionner les deux DataFrames agrégés
    agg_df = agg_1m_df.join(
        agg_5m_df,
        on="Symbol",
        how="outer"
    )

    # Écrire les données agrégées dans PostgreSQL
    query = agg_df.writeStream \
        .outputMode("update") \
        .foreachBatch(write_to_postgresql) \
        .start()

    # Attendre la terminaison de la requête
    query.awaitTermination()

if __name__ == "__main__":
    main()
