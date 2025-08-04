from pyspark.sql import SparkSession

if __name__=="__main__":
    spark = SparkSession.builder \
        .appName("Streaming Postgres") \
        .getOrCreate()
    
    jsonschema = "nome STRING, postagem STRING, data INT" \

    # Example of reading from a streaming source
    df = spark.readStream \
        .json("/home/robson/curso_spark_pyspark_udemy/Pratica/Streaming/testestream/", schema=jsonschema)
    
    diretorio = "/home/robson/curso_spark_pyspark_udemy/Pratica/Streaming/temp/"

    def atualiza_postgres(df_posts, batch_id):
        df_posts.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/posts") \
            .option("dbtable", "posts") \
            .option("user", "postgres") \
            .option("password", "123456") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
    StCal = df.writeStream.foreachBatch(atualiza_postgres) \
            .outputMode("append") \
            .trigger(processingTime="5 seconds") \
            .option("checkpointLocation", diretorio) \
            .start()
    
    StCal.awaitTermination()
    