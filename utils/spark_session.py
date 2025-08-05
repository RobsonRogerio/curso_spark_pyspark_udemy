from pyspark.sql import SparkSession
import os

def get_spark_session(app_name):
    warehouse_location = "/home/robson/curso_spark_pyspark_udemy/Pratica/Spark-SQL/spark-warehouse"
    metastore_path = "/home/robson/curso_spark_pyspark_udemy/Pratica/Spark-SQL/metastore_db"

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.warehouse.dir", warehouse_location) \
        .config("javax.jdo.option.ConnectionURL", f"jdbc:derby:;databaseName={metastore_path};create=true") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark