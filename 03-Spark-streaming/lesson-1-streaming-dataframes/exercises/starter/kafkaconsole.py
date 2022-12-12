from pyspark.sql import SparkSession

#TO-DO: create a Spak Session, and name the app something relevant
spark = SparkSession.builder\
    .master("spark://spark-streaming-spark-master:7077")\
    .appName("SparkStreaming")\
    .getOrCreate()
    
#TO-DO: set the log level to WARN
spark.sparkContext.setLogLevel("WARN")

#TO-DO: read a stream from the kafka topic 'balance-updates', with the bootstrap server kafka:19092, reading from the earliest message
raw_stream = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "spark-streaming-kafka:19092")\
    .option("subscribe", "balance-updates")\
    .option("startingOffsets", "earliest")\
    .load()


#TO-DO: cast the key and value columns as strings and select them using a select expression function
stream_df = raw_stream\
    .selectExpr("cast(key as string) key", "cast(value as string) value")

#TO-DO: write the dataframe to the console, and keep running indefinitely
stream_df.writeStream \
    .outputMode("append")\
    .format("console")\
    .start() \
    .awaitTermination()