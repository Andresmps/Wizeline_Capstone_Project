import sqlalchemy
import joblib
import pg8000
import json
import os
import re

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession 
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from google.cloud import storage

# GCS paths

ENV = "dev"
BUCKET_NAME = f"capstone-project-{ENV}"

# Input
MOVIE_REVIEW_PATH = \
    f"gs://{BUCKET_NAME}/Raw/movie_review.csv"

LOG_REVIEW_PATH = \
    f"gs://{BUCKET_NAME}/Raw/log_reviews.csv"

INSTANCE_CREDENTIALS = \
    f"gs://{BUCKET_NAME}/Others/env_vars.json"

STOPWORDS_PATH = \
    f"gs://{BUCKET_NAME}/Others(stopwords_english.pkl"

REGIONS_PATH = \
    f"gs://{BUCKET_NAME}/Others/regions.csv"


# Output
USER_PURCHASE_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/user_purchase/"

MOVIE_REVIEW_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/movie_review/"

LOG_REVIEW_STAGE_PATH = \
    f"gs://{BUCKET_NAME}/Staging/log_reviews/"


SEASONS = {
    "Winter1": [101, 320],
    "Spring": [320, 621],
    "Summer": [621, 923],
    "Fall": [923, 1221],
    "Winter2": [1221, 1232],
}

# Queries
QUERY_USER_PURCHASE = """
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


def read_file_from_gcs(gcs_path):

    storage_client = storage.Client()
    bucket, blob_name = get_bucket_blobname(gcs_path)
    filename = blob_name.split('/')[-1]

    bucket_ = storage_client.bucket(bucket)
    blob = bucket_.blob(blob_name)
    blob.download_to_filename(filename)

    return filename


def read_json_from_gcs(gcs_path):

    filename = read_file_from_gcs(gcs_path)
    data = json.loads(open(filename, 'r').read())
    os.remove(filename)

    return data


def read_pickle_from_gcs(gcs_path):

    filename = read_file_from_gcs(gcs_path)
    data = joblib.load(filename)
    os.remove(filename)

    return data


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


def get_bucket_blobname(gcs_path):
    gcs_path_ = re.match("gs://([^/]+)/(.+)", gcs_path)
    bucket, blob_name = gcs_path_.group(1), gcs_path_.group(2)
    
    return bucket, blob_name

def init_tcp_connection_engine(db_config):

    env_vars = read_json_from_gcs(INSTANCE_CREDENTIALS)

    pool = sqlalchemy.create_engine(
        # Equivalent URL:
        # postgresql+pg8000://<db_user>:<db_pass>@<db_host>:<db_port>/<db_name>
        sqlalchemy.engine.url.URL.create(
            drivername="postgresql+pg8000",
            username=env_vars["DB_USER"],  # e.g. "my-database-user"
            password=env_vars["DB_PASS"],  # e.g. "my-database-password"
            host=env_vars["DB_HOST"],  # e.g. "127.0.0.1"
            port=env_vars["DB_PORT"],  # e.g. 5432
            database=env_vars["DB_NAME"]  # e.g. "my-database-name"
        ),
        **db_config
    )

    return pool


def clean_user_purchase_table(df_user_purchase):

    dup_cols = ["customer_id", "invoice_number", "stock_code", "invoice_date"]

    print(
        "Before cleaning user_purchase table total of rows:"
        f" {df_user_purchase.count()}"
    )

    try:
        # Remove non-identifible customers
        df_user_purchase_ = df_user_purchase\
            .filter(F.col("customer_id") != -1)

        # Remove Discounts (D) and Manual (M) aggregations
        df_user_purchase_2 = df_user_purchase_\
            .filter(
                F.length(F.col('stock_code')) != 1
            )

        # Remove negative quantities, negative unit prices and invoices returned
        df_user_purchase_3 = df_user_purchase_2\
            .filter(
                ~(
                    (F.col('quantity') < 0) |
                    (F.col('invoice_number').rlike(r"^\D")) |
                    (F.col("unit_price") < 0)
                )
            )

        # Remove duplicates
        df_user_purchase_4 = df_user_purchase_3\
            .drop_duplicates(dup_cols)

        # Add amount column
        df_user_purchase_5 = df_user_purchase_4.withColumn(
            'amount', F.round(F.col('unit_price') * F.col('quantity'), 4)
        )

        # Schema correction
        df_user_purchase_final = df_user_purchase_5\
            .select(
                F.col('customer_id').cast('int'),
                F.regexp_replace(
                    F.col('invoice_number'), "\D", ""
                    )\
                    .alias('invoice_number').cast('int'),
                F.col('stock_code'),
                F.col('invoice_date').cast('date').alias('invoice_date'),
                F.trim(F.col('detail')).alias('detail'),
                F.col('quantity'),
                F.col('unit_price'),
                F.col('country')
                
            )

        print(
            "After cleaning user_purchase table total of rows:"
            f" {df_user_purchase_final.count()}"
        )

        return df_user_purchase_final

    except Exception as e:
        print(f"An exception was raise due to error: {e}")

    return df_user_purchase


def load_table_from_cloudsql_to_gcs(query):

    # Start CloudSQL connection
    db = init_connection_engine()

    # Extract all records with a query
    with db.connect() as conn:
        results = conn.execute(query).fetchall()
        results = [result._asdict() for result in results]

    df_results = spark.createDataFrame(results)
    df_clean_results = clean_user_purchase_table(df_results)

    print(USER_PURCHASE_STAGE_PATH)
    df_clean_results\
        .write.mode('overwrite')\
        .parquet(USER_PURCHASE_STAGE_PATH)


def clean_movie_review_table():

    # Load NLTK english stopworks previously loaded to GCS
    stopwords_english = read_pickle_from_gcs(STOPWORDS_PATH)

    # Read movie review table from GCS
    df_movie_review = spark\
        .read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(MOVIE_REVIEW_PATH)
    
    # Remove unnecesary punctuation and characters
    df_movie_review_ = df_movie_review\
        .withColumn(
            "review_str_clean",
            F.regexp_replace(
                F.regexp_replace(F.col('review_str'), r"[^a-zA-Z0-9! ]", ""),
                r"[ ]{2,}", " "
            )
        )

    # Tokenizer of words
    tokenizer = Tokenizer(inputCol="review_str", outputCol="words")

    # Stopwords remover
    remover = StopWordsRemover(
        inputCol="words", outputCol="words_final",
        stopWords=stopwords_english
    )

    df_movie_review_tok = tokenizer.transform(df_movie_review_)
    df_movie_review_sw = remover.transform(df_movie_review_tok)

    df_movie_review_pr = df_movie_review_sw\
        .withColumn(
            "positive_review", F.when(
                F.array_contains(F.col('words_final'), "good"), 1
                ).otherwise(0)
        )

    df_movie_review_final = df_movie_review_pr\
        .select(
            F.col("cid").alias("customer_id").cast("int"),
            F.col("positive_review"),
            F.col("id_review").alias("review_id").cast("int")
        )

    df_movie_review_final\
        .write.mode('overwrite')\
        .parquet(MOVIE_REVIEW_STAGE_PATH)


def read_xml_load_to_gcs(row_tag, root_tag):

    # Read xml column
    df_log_review = spark\
        .read\
        .option("rowTag", row_tag)\
        .option("rootTag", root_tag)\
        .format("xml")\
        .load(LOG_REVIEW_PATH)


    regions_usa = spark\
        .read\
        .option('header', True)\
        .csv(REGIONS_PATH)

    # review_id addition
    window_ = Window().orderBy(F.lit('A'))
    df_log_review = df_log_review\
        .withColumn(
            "id_review", F.row_number().over(window_)
        )

    # Browser addition
    df_log_review_ = df_log_review\
        .withColumn(
            "browser", F.when(
                F.col('os') == "Linux", "Firefox"
            ).otherwise(
                F.when(
                    F.col('os') == "Google Android", "Chrome"
                ).otherwise(
                    F.when(
                        F.col('os') == "Microsoft Windows", "Edge"
                    ).otherwise(
                        "Safari"
                    )
                )
            )
        )

    df_log_review_stage = df_log_review_\
        .join(
            regions_usa, on='location', how='left'
        )

    # Seasons addition
    df_log_review_final = df_log_review_final\
        .withColumn(
            "month_day",
            F.date_format(
                F.to_date(F.col("logDate"), format="MM-dd-yyyy"),
                'MMdd'
            ).cast('int')
        )

    df_log_review_final = df_log_review_final\
        .withColumn(
            'season', F.when(
                (F.col('month_day') >= SEASONS['Spring'][0]) &
                (F.col('month_day') < SEASONS['Spring'][1]), "Spring"
            ).otherwise(
                F.when(
                    (F.col('month_day') >= SEASONS['Summer'][0]) &
                    (F.col('month_day') < SEASONS['Summer'][1]), "Summer"
                ).otherwise(
                    F.when(
                        (F.col('month_day') >= SEASONS['Fall'][0]) &
                        (F.col('month_day') < SEASONS['Fall'][1]), "Fall"
                    ).otherwise(
                        "Winter"
                    )   
                )    
            )
        )\
        .drop('month_day')

    # Schema adjustments

    df_log_review_final = df_log_review_stage\
        .select(
            F.col("id_review").cast("int").alias("review_id"),
            F.col("device"),
            F.col("os"),
            F.col("location"),
            F.to_date(F.col("logDate"), format="MM-dd-yyyy").alias("log_date"),
            F.col("phone_number"),
            F.col("ip_address"),
            F.col("browser"),
            F.col("region"),
        )
    

    print(f"Log reviews count: {df_log_review.count()}")
    print(f"Log reviews columns: {df_log_review.columns}")
    print(f"Log reviews first record: {df_log_review.first()}")

    df_log_review_final\
        .write.mode('overwrite')\
        .parquet(LOG_REVIEW_STAGE_PATH)


if __name__ == "__main__":
    load_table_from_cloudsql_to_gcs(QUERY_USER_PURCHASE)
    clean_movie_review_table()
    read_xml_load_to_gcs("log", "reviewlog")
