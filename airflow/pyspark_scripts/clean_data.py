import sqlalchemy
import joblib
import pg8000
import os
import re

from pyspark.sql import DataFrame, SparkSession 
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, size, when, array_contains, row_number, lit
from pyspark.sql.window import Window

from google.cloud import storage

# GCS paths
# Input
movie_review_path = \
    "gs://data-bootcamp-test-1-dev-data/Raw/movie_review.csv"

log_review_path = \
    "gs://data-bootcamp-test-1-dev-data/Raw/log_reviews.csv"

stopwords_path = \
    "gs://data-bootcamp-test-1-dev-data/stopwords_english.pkl"

# Output
user_purchase_staging_path = \
    "gs://data-bootcamp-test-1-dev-data/Staging/user_purchase/"

movie_review_staging_path = \
    "gs://data-bootcamp-test-1-dev-data/Staging/movie_review/"

log_review_staging_path = \
    "gs://data-bootcamp-test-1-dev-data/Staging/log_reviews/"


# Queries
query_user_purchase = """
    SELECT 
        invoice_number, 
        stock_code, 
        detail, 
        quantity,
        invoice_date, 
        CAST( unit_price AS FLOAT),
        customer_id,
        country
    FROM user_purchase
"""

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Data cleaning") \
    .getOrCreate()


def init_connection_engine():
    db_config = {
        "pool_size": 5, # Pool size is the maximum number of permanent connections to keep.
        "max_overflow": 2, # Temporarily exceeds the set pool_size if no connections are available.
        "pool_timeout": 30, # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
                            #    new connection from the pool
        "pool_recycle": 1800,  # 'pool_timeout' is the maximum number of seconds to wait when retrieving a
                               #    new connection from the pool
    }

    return init_tcp_connection_engine(db_config)
    

def init_tcp_connection_engine(db_config):

    db_user = os.environ["DB_USER"]
    db_pass = os.environ["DB_PASS"]
    db_name = os.environ["DB_NAME"]
    db_host = os.environ["DB_HOST"]
    db_port = os.environ["DB_PORT"]

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # postgresql+pg8000://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=db_user,  # e.g. "my-database-user"
            password=db_pass,  # e.g. "my-database-password"
            host=db_host,  # e.g. "127.0.0.1"
            port=db_port,  # e.g. 5432
            database=db_name  # e.g. "my-database-name"
        ),
        **db_config
    )

    return pool


def get_bucket_blobname(gcs_path):
    gcs_path_ = re.match("gs://([^/]+)/(.+)", gcs_path)
    bucket, blob_name = gcs_path_.group(1), gcs_path_.group(2)
    
    return bucket, blob_name


def read_pickle_from_gcs(gcs_path):

    storage_client = storage.Client()
    bucket, blob_name = get_bucket_blobname(stopwords_path)
    filename = blob_name.split('/')[-1]

    bucket_ = storage_client.bucket(bucket)
    blob = bucket_.blob(blob_name)
    blob.download_to_filename(filename)

    data = joblib.load(filename)
    os.remove(filename)

    return data


def load_file_from_cloudsql_to_gcs(query, output_file):

    db = init_connection_engine()

    with db.connect() as conn:
        results = conn.execute(query).fetchall()
        results = [result._asdict() for result in results]

    df_results = spark.createDataFrame(results)

    print(output_file)
    df_results\
        .write.mode('overwrite')\
        .parquet(output_file)


def clean_movie_review_file():
    df_movie_review = spark.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(movie_review_path)

    tokenizer = Tokenizer(inputCol="review_str", outputCol="words")
    stopwords_english = read_pickle_from_gcs(stopwords_path)

    remover = StopWordsRemover(
        inputCol="words", outputCol="words_final",
        stopWords=stopwords_english
    )

    df_movie_review_tok = tokenizer.transform(df_movie_review)
    df_movie_review_sw = remover.transform(df_movie_review_tok)

    df_movie_review_pr = df_movie_review_sw.withColumn(
        "positive_review", when(
            array_contains(col('words_final'), "good"), 1
            ).otherwise(0)
    )

    df_movie_review_final = df_movie_review_pr.select(
        col("cid").alias("user_id"),
        col("positive_review"),
        col("id_review").alias("review_id")
    )

    df_movie_review_final\
        .write.mode('overwrite')\
        .parquet(movie_review_staging_path)


def read_xml_load_to_gcs(gcs_path, row_tag, root_tag):
    df_log_review = spark.read\
        .option("rowTag", row_tag)\
        .option("rootTag", root_tag)\
        .format("xml")\
        .load(log_review_path)

    window_ = Window().orderBy(lit('A'))
    df_log_review = df_log_review.withColumn(
        "id_review", row_number().over(window_)
    )

    print(df_log_review.count())
    print(df_log_review.columns)
    print(df_log_review.first())

    df_log_review\
        .write.mode('overwrite')\
        .parquet(log_review_staging_path)


load_file_from_cloudsql_to_gcs(
    query_user_purchase, user_purchase_staging_path
)

clean_movie_review_file()
read_xml_load_to_gcs(log_review_path, "log", "reviewlog")
