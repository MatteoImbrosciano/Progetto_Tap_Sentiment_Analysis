from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType, DoubleType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import from_json, col, create_map, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_json, col
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel
from pyspark.sql.functions import from_unixtime, unix_timestamp, hour, minute
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, minute, from_unixtime, unix_timestamp, udf
from pyspark.sql.types import StringType
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, unix_timestamp, hour, minute, udf, concat_ws
from pyspark.sql.types import StringType, DoubleType
from pyspark.ml.classification import MultilayerPerceptronClassificationModel
from pyspark.ml.feature import StringIndexerModel, VectorAssembler
from pyspark.sql.functions import struct
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, array
import sparknlp 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, BooleanType
from sparknlp.annotator import Tokenizer, DeBertaForSequenceClassification
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover
from pyspark.ml.classification import LogisticRegression

# Import the required modules and classes
from sparknlp.base import DocumentAssembler, Pipeline, Finisher
from sparknlp.annotator import (
    SentenceDetector,
    Tokenizer,
    Lemmatizer,
    SentimentDetector
)
import pyspark.sql.functions as F
from sparknlp.annotator import Tokenizer, BertSentenceEmbeddings, ClassifierDLModel

# Configurazione Spark
sparkconf = SparkConf()\
    .set("es.nodes", "elasticsearch") \
    .set("es.port", "9200")

spark = SparkSession.builder.appName("kafkatospark").config(conf=sparkconf).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Kafka e ElasticSearch Config
kafkaServer = "broker:9092"
topic = "pipe"
elastic_index = "data"

# Definisci lo schema
schema = StructType([
    StructField("@timestamp", StringType(), True),
    StructField("msg", StringType(), True),
    StructField("continuation", StringType(), True),
    StructField("commentsCount", StringType(), True),
    StructField("@version", StringType(), True),
    StructField("data", StructType([
        StructField("publishedAt", StringType(), True),
        StructField("authorChannelId", StringType(), True),
        StructField("authorThumbnail", ArrayType(StructType([
            StructField("height", IntegerType(), True),
            StructField("width", IntegerType(), True),
            StructField("url", StringType(), True)
        ])), True),
        StructField("likesCount", StringType(), True),
        StructField("textDisplay", StringType(), True),
        StructField("publishedTimeText", StringType(), True),
        StructField("isVerified", BooleanType(), True),
        StructField("replyCount", StringType(), True),
        StructField("authorText", StringType(), True),
        StructField("isArtist", BooleanType(), True),
        StructField("isCreator", BooleanType(), True),
        StructField("authorIsChannelOwner", BooleanType(), True),
        StructField("commentId", StringType(), True),
        StructField("publishDate", StringType(), True)
    ]), True),
    StructField("event", StructType([
        StructField("original", StringType(), True)
    ]), True)
])

#lettura del topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

df.printSchema()
# Trasformazione del dataframe
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data"))

print("Schema dopo from_json:")
df.printSchema()

# Selezione dei campi specifici dalla struttura annidata
df = df.select("data.@timestamp", "data.data.publishedTimeText", "data.data.textDisplay", "data.data.authorText", "data.data.commentId")


print("stampa finale")
    
# Stampa i primi pochi record per il debug
df.writeStream \
    .format("console") \
    .start() \
    .awaitTermination()
    
# Write the result to Elasticsearch
#query = df_final.writeStream \
#    .format("org.elasticsearch.spark.sql") \
#    .option("es.resource", "data") \
#    .option("checkpointLocation", "/tmp/checkpoints") \
#    .option("es.nodes", "elasticsearch") \
#    .option("es.port", "9200") \
#    .start("data") \
#    .awaitTermination()