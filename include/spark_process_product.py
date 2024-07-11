from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import monotonically_increasing_id, col
import sys, ast, logging, os, shutil

logging.basicConfig(level=logging.INFO)

def move_file_complete(file_path:str):
    file_name = os.path.basename(file_path)
    complete_dir = './data/product_catalog/complete'
    shutil.move(file_path, os.path.join(complete_dir, file_name))
    logging.info(f'nove file to {os.path.join(complete_dir, file_name)}')

def clean_data(df, file_path:str):
    from datetime import datetime
    df_trans = df.select('*')
    df_trans = df_trans.withColumn("price", col("price").cast(DoubleType()))
    df = df.withColumn("index", monotonically_increasing_id())
    df_trans = df_trans.withColumn("index", monotonically_increasing_id())
    bad_records = df_trans.filter((col("price") < 0) | (col("price").isNull()))
    if bad_records.count() > 0:
        bad_indices = bad_records.select("index").rdd.flatMap(lambda x: x).collect()
        df_invalid = df.filter(col("index").isin(bad_indices)).drop('index')
        
        # export bad records
        current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = os.path.basename(file_path).rsplit('.', 1)[0]
        tmp_dir = f"/tmp/bad_records_{current_datetime}"
        output_dir = f"./data/product_catalog/error"
        new_file_name = f"invalid_{file_name}_{current_datetime}.csv"
        
        # export csv to /tmp
        df_invalid.coalesce(1).write.csv(tmp_dir, header=True, mode="overwrite")
        
        # move to output_dir
        csv_file = [f for f in os.listdir(tmp_dir) if f.endswith(".csv")][0]
        shutil.move(os.path.join(tmp_dir, csv_file), os.path.join(output_dir, new_file_name))
        logging.info(f'export invalid complete {os.path.join(output_dir, new_file_name)}')
    df = df.filter(~col("index").isin(bad_indices)).drop('index')
    df = df.withColumn("price", col("price").cast(DoubleType()))
    return df

# get environment var
db_username= os.environ.get('DB_POSTGRES_USERNAME')
db_password= os.environ.get('DB_POSTGRES_PASSWORD')
db_name= os.environ.get('DB_POSTGRES_DATABASE')
db_host= os.environ.get('DB_POSTGRES_HOST')

spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True)
])

jdbc_url = f"jdbc:postgresql://{db_host}:5432/{db_name}"

# get value from application_args
list_file_path = sys.argv[1]
list_file_path = ast.literal_eval(list_file_path)

for file_path in list_file_path:
    logging.info(f'Start process file {file_path}')
    # df = spark.read.schema(schema).csv(file_path)
    df = spark.read.format("csv").option("header", "true").schema(schema).load(file_path)
    df = clean_data(df, file_path)
    df.write.format("jdbc")\
        .mode("append")\
        .option("url", jdbc_url)\
        .option("driver", "org.postgresql.Driver")\
        .option("dbtable", "product_catalog")\
        .option("user", db_username)\
        .option("password", db_password)\
        .save()
    logging.info(f'ingestion complete {file_path}')
    # move_file_complete(file_path)

spark.stop()