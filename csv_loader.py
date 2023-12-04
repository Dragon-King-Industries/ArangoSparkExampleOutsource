import os
from pyspark.sql import SparkSession
from arango import ArangoClient
from pyspark.sql.functions import lit
from dotenv import load_dotenv

load_dotenv()

def insert_to_arangoDB(row):
    doc = dict(row)
    collection.insert(doc)

spark = SparkSession.builder.appName('CSV to ArangoDB').getOrCreate()
df = spark.read.option("mode", "DROPMALFORMED").csv(os.getenv('CSV_PATH'), inferSchema = True, header = True)

# This will add a constant column with collection name for grouping
df = df.withColumn('collection', lit(os.getenv('ARANGO_COLLECTION'))).cache()

# Write to ArangoDB
client = ArangoClient(hosts=os.getenv('ARANGO_HOST'))
db = client.db(os.getenv('ARANGO_DB'), username=os.getenv('ARANGO_USER'), password=os.getenv('ARANGO_PASS'))
collections = df.select('collection').distinct().collect()

for collection_row in collections:
    collection = db.collection(collection_row.collection)
    collection_data = df.where(df.collection == collection_row.collection)
    collection_data.foreach(insert_to_arangoDB)

spark.stop()