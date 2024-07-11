from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import col
import sys, ast, logging, os, shutil, json

def move_file_complete(file_path:str):
    file_name = os.path.basename(file_path)
    complete_dir = './data/customer_transaction/complete'
    shutil.move(file_path, os.path.join(complete_dir, file_name))
    logging.info(f'nove file to {os.path.join(complete_dir, file_name)}')

# get environment var
db_username= os.environ.get('DB_POSTGRES_USERNAME')
db_password= os.environ.get('DB_POSTGRES_PASSWORD')
db_name= os.environ.get('DB_POSTGRES_DATABASE')
db_host= os.environ.get('DB_POSTGRES_HOST')

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

jdbc_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

# get value from application_args
list_file_path = sys.argv[1]
list_file_path = ast.literal_eval(list_file_path)

for file_path in list_file_path:
    print(file_path)
    raw_json = json.load(open(file_path, 'r'))
    df = spark.createDataFrame(raw_json, schema)
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

    df.write.format("jdbc")\
        .mode("append")\
        .option("url", jdbc_url)\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "customer_transaction")\
        .option("user", db_username)\
        .option("password", db_password)\
        .save()
    
    logging.info(f'ingestion complete {file_path}')
    move_file_complete(file_path)

spark.stop()
