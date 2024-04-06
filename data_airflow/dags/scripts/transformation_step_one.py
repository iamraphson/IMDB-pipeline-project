import logging
import argparse
from pyspark.sql import SparkSession, types, functions as F

parser = argparse.ArgumentParser()

parser.add_argument('--title_basics_src', required=True)
parser.add_argument('--title_ratings_src', required=True)
parser.add_argument('--fact_movies_pq_dest', required=True)
parser.add_argument('--fact_genres_pq_dest', required=True)
parser.add_argument('--fact_movies_table_dest', required=True)
parser.add_argument('--dim_genres_table_dest', required=True)

args = parser.parse_args()

spark = SparkSession.builder \
        .appName('init-data-transformation') \
        .getOrCreate()


spark.conf.set('temporaryGcsBucket', 'dataproc-temp-us-west1-475254441817-u7kv6jeo')

title_basics_df = spark.read.parquet(args.title_basics_src)
title_basics_df.createOrReplaceTempView('title_basics_data')

title_ratings_df = spark.read.parquet(args.title_ratings_src)
title_ratings_df.createOrReplaceTempView('title_ratings_data')

facts_movies_df = spark.sql(
"""
WITH tr AS (
    SELECT
        *
    FROM
      title_ratings_data
),
tb AS (
    SELECT
        *
    FROM
      title_basics_data
)

SELECT
    tb.tconst,
    title_type,
    primary_title,
    original_title,
    is_adult,
    start_year,
    end_year,
    runtime_minutes,
    genres,
    average_rating,
    number_of_votes,
    imdb_link
FROM tb INNER JOIN tr
    ON tb.tconst = tr.tconst
"""
)

facts_movies_df.repartition(20).write.parquet(args.fact_movies_pq_dest, mode='overwrite')
facts_movies_df.write.format('bigquery').option('table', args.fact_movies_table_dest).mode("overwrite").save()
facts_movies_df.createOrReplaceTempView('facts_movies_data')


dim_genres_df = spark.sql(
"""
WITH fm AS (
    SELECT
        *
    FROM facts_movies_data
),
fm_genre AS (
    SELECT
        tconst,
        EXPLODE(SPLIT(genres, ',')) AS genre
    FROM facts_movies_data
)

SELECT
    fm_genre.genre as genres_name,
    count(1) as num_of_films
FROM fm INNER JOIN fm_genre
ON fm.tconst = fm_genre.tconst
GROUP BY 1
"""
)

dim_genres_df.repartition(1).write.parquet(args.fact_genres_pq_dest, mode='overwrite')
dim_genres_df.write.format('bigquery').option('table', args.dim_genres_table_dest).mode("overwrite").save()
