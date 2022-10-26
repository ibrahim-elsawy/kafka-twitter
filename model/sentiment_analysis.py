from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
# from textblob import TextBlob
# from pysentimiento import create_analyzer
from confluent_kafka import Producer
# import torch

import asyncio
import websockets

uri = "ws://localhost:8765"


# device = torch.device("cpu")
# analyzer = create_analyzer(task="sentiment", lang="en")
# analyzer.model.to(device)



p = Producer({'bootstrap.servers': 'localhost:9092'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(
            msg.topic(), msg.partition()))


def preprocessing(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words = words.withColumn('word', F.regexp_replace('word', r'http\S+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '@\w+', ''))
    words = words.withColumn('word', F.regexp_replace('word', '#', ''))
    words = words.withColumn('word', F.regexp_replace('word', 'RT', ''))
    words = words.withColumn('word', F.regexp_replace('word', ':', ''))
    return words

# text classification
async def get_from_socket(text):
    async with websockets.connect(uri) as websocket:
        await websocket.send(text)
        data = await websocket.recv()
        return data



def udf_wrapper(topicName, dataId):


    def polarity_detection(text):
        # return analyzer.predict(text).output
        data = asyncio.run(get_from_socket(text))
        send_to_kafka(data, topicName, dataId)
        return data

    return polarity_detection



def text_classification(words, topicName, dataId):
    # polarity detection
    polarity_detection_udf = udf(udf_wrapper(topicName, dataId), StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # send_summary(words, topicName, dataId)
    return words


# def get_summary_db(words):

#     summary = {}
#     for key in ["NEG", "NEU", "POS"]:
#         summary[key] = words.filter(words.polarity == key).count()
#     return spark.createDataFrame([
#         ("NEG"+summary["NEG"],),
#         ("NEU"+summary["NEU"],),
#         ("POS"+summary["POS"],)], schema='a string')


# def send_summary(data, words, topicName, dataId):
#     print('Kafka Producer has been initiated...')
#     for key in ["NEG", "NEU", "POS"]:
#         data = words.filter(words.polarity == key).count()
#         data = key + "#*#" + data + "#*#" + dataId
#         p.poll(0)
#         p.produce(topicName, data.encode('utf-8'), callback=delivery_report)
#         p.flush()

def send_to_socket():
    pass

def send_to_kafka(data, topicName, dataId):
        data = data + "#*#" + dataId
        p.poll(0)
        p.produce(topicName, data.encode('utf-8'), callback=delivery_report)
        p.flush()


def do_sent_analysis(topicName, dataId):
    # create Spark session
    spark = SparkSession.builder.appName(
        "TwitterSentimentAnalysis").getOrCreate()

    # read the tweet data from socket
    lines = spark.readStream.format("socket").option(
        "host", "0.0.0.0").option("port", 5555).load()
    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words, topicName, dataId)

    # send_summary(words, topicName, dataId)
    words = words.repartition(1)

    # query = words.writeStream .format("csv") .trigger(processingTime="10 seconds") .option("checkpointLocation", "checkpoint/") .option("path", "output_path/") .outputMode("append") .start() .awaitTermination()
    query = words.writeStream.format("console").start()
    # query.awaitTermination()
    # query = words.writeStream.queryName("all_tweets")\
    #     .outputMode("append").format("parquet")\
    #     .option("path", "./parc")\
    #     .option("checkpointLocation", "./check")\
    #     .trigger(processingTime='60 seconds').start()
    # query.awaitTermination()


if __name__ == "__main__":
    # create Spark session
    spark = SparkSession.builder.appName(
        "TwitterSentimentAnalysis").getOrCreate()

    # read the tweet data from socket
    lines = spark.readStream.format("socket").option(
        "host", "0.0.0.0").option("port", 5555).load()
    # Preprocess the data
    words = preprocessing(lines)
    # text classification to define polarity and subjectivity
    words = text_classification(words)

    words = words.repartition(1)
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("parquet")\
        .option("path", "./parc")\
        .option("checkpointLocation", "./check")\
        .trigger(processingTime='60 seconds').start()
    query.awaitTermination()
