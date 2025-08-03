from pyspark.sql import SparkSession

if __name__=="__main__":
    spark = SparkSession.builder \
        .appName("Streaming Example") \
        .getOrCreate()
    
    jsonschema = "nome STRING, postagem STRING, data INT" \

    # Example of reading from a streaming source
    df = spark.readStream \
        .json("/home/robson/curso_spark_pyspark_udemy/Pratica/Streaming/testestream/", schema=jsonschema)
    
    diretorio = "/home/robson/curso_spark_pyspark_udemy/Pratica/Streaming/temp/"

    # Example of writing to a console sink
    stcal = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime='5 seconds') \
        .option("checkpointLocation", diretorio) \
        .start()

    stcal.awaitTermination()