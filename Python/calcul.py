#spark streaming pour recuperer les données de Kafka et faire les aggregation de données, retirer les transaction d'erreur ...
#envoyé la valeur de sortie dans un bucket minio
#2 scd
#faire un consumer python qui va recuperer les evenement pour le mettre dans un fichier json
#utiliser spark ou server

import os
import json
import time
import io
import tempfile
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import regexp_replace, split, lit, concat, when, trim
from minio import Minio
from dotenv import load_dotenv

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'

load_dotenv()

bootstrap_server = os.getenv("KAFKA_BOOTSTRAP_SERVER")
topic_name = os.getenv("KAFKA_TOPIC_NAME")
minio_server = os.getenv("MINIO_SERVER")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
minio_bucket_name = os.getenv("MINIO_BUCKET_NAME")

def write_data_minio(filename : str, data):
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        minio_server,
        secure=False,
        access_key=minio_access_key,
        secret_key=minio_secret_key
    )
    bucket: str = minio_bucket_name
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
    client.put_object(bucket, filename, data, length=-1, part_size=10*1024 * 1024)

def get_prefix_filename():
    return f"kafka_extract_{time.strftime('%Y-%m-%d-%H%M%S')}"

def send_directory_file_to_minino(directory : str):
    # parse the file parquet in the directory
    index_file = 0
    for filename_tmp in os.listdir(directory):
            # exclude file wich is not parquet
            if filename_tmp[-8::] != ".parquet":
                continue
            path = os.path.join(directory, filename_tmp)
            filename = f"{get_prefix_filename()}_{index_file}.parquet"
            index_file += 1
            with open(path, "rb") as parquet_file:
                # read the file parquet and send it to Minio server
                binary_data : bytes = parquet_file.read()
                write_data_minio(filename=filename, data=io.BytesIO(binary_data))

def send_data_to_minio(data: DataFrame):
    # change dataframe to byte and send to write_data_minio
    with tempfile.TemporaryDirectory() as temp_dir:
        data.write.format("parquet").save(temp_dir, mode="overwrite")
        send_directory_file_to_minino(directory=temp_dir)

def change_format_of_data(raw_df : DataFrame) -> DataFrame:
    # change USD to EUR
    changed_data : DataFrame = raw_df.withColumn("devise", regexp_replace('devise', 'USD', 'EUR'))
    #change date string to date Date
    changed_data = changed_data.withColumn("date", changed_data.date.cast("date"))
    # Create column timezone with the use of the column "lieu" with witdrawing the name of the street
    changed_data = changed_data.withColumn("timezone",
        when(
         split(changed_data["lieu"], ",").getItem(1).isNotNull(), # check if the column "lieu" is correct
         concat(lit("Europe/"), trim(split(changed_data["lieu"], ", ")[1].cast("string"))) # get the country in the column "lieu" and concat with europe
        ).otherwise("Unknow")
    )
    # Delete "moyen_paiement" error value
    changed_data = changed_data.filter(changed_data.moyen_paiement != "erreur")
    # Delete "lieu" wich contains None in the value
    changed_data = changed_data.filter(changed_data.lieu.contains("None") == False)
    # Delete "Adresse" wich contains None
    changed_data = changed_data.filter(changed_data.utilisateur["adresse"].contains("None") == False)
    return changed_data

def handle_data(value : DataFrame):
    format_data : DataFrame = change_format_of_data(raw_df=value)
    send_data_to_minio(format_data)

def handle_batch_data(batch_df : DataFrame, id : int):
    value_list = batch_df.select("value").collect() # extract value from the batch
    if len(value_list) == 0:
        return
    # create spark Dataframe from the json of kafka
    value_df : DataFrame = spark.createDataFrame([Row(**json.loads(value.value)) for value in value_list])
    handle_data(value_df)

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

df : DataFrame = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", bootstrap_server)\
    .option('subscribe', topic_name)\
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)", "timestamp")

query = df.writeStream.foreachBatch(handle_batch_data).start()

query.awaitTermination()

