import argparse
import logging
from pyspark.sql import SparkSession, types, functions as F

name_basics_schema = types.StructType([
    types.StructField('nconst',types.StringType(),True),
    types.StructField('primaryName',types.StringType(),True),
    types.StructField('birthYear',types.IntegerType(),True),
    types.StructField('deathYear',types.IntegerType(),True),
    types.StructField('primaryProfession',types.StringType(),True),
    types.StructField('knownForTitles',types.StringType(),True)
])

title_akas_schema = types.StructType([
    types.StructField('titleId',types.StringType(),True),
    types.StructField('ordering',types.IntegerType(),True),
    types.StructField('title',types.StringType(),True),
    types.StructField('region',types.StringType(),True),
    types.StructField('language',types.StringType(),True),
    types.StructField('types',types.StringType(),True),
    types.StructField('attributes',types.StringType(),True),
    types.StructField('isOriginalTitle',types.IntegerType(),True)
])

title_basics_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('titleType',types.StringType(),True),
    types.StructField('primaryTitle',types.StringType(),True),
    types.StructField('originalTitle',types.StringType(),True),
    types.StructField('isAdult',types.IntegerType(),True),
    types.StructField('startYear',types.IntegerType(),True),
    types.StructField('endYear',types.IntegerType(),True),
    types.StructField('runtimeMinutes',types.IntegerType(),True),
    types.StructField('genres',types.StringType(),True)
])

title_crew_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('directors',types.StringType(),True),
    types.StructField('writers',types.StringType(),True)
])

title_episode_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('parentTconst',types.StringType(),True),
    types.StructField('seasonNumber',types.IntegerType(),True),
    types.StructField('episodeNumber',types.IntegerType(),True)
])

title_principals_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('ordering',types.IntegerType(),True),
    types.StructField('nconst',types.StringType(),True),
    types.StructField('category',types.StringType(),True),
    types.StructField('job',types.StringType(),True),
    types.StructField('characters',types.StringType(),True)
])

title_rating_schema = types.StructType([
    types.StructField('tconst',types.StringType(),True),
    types.StructField('averageRating',types.DoubleType(),True),
    types.StructField('numVotes',types.IntegerType(),True)
])

def determineDatasetSchema(selected_data):
    if selected_data == 'name_basics':
        return name_basics_schema
    elif selected_data == 'title_basics':
        return title_basics_schema
    elif selected_data == 'title_principals':
        return title_principals_schema
    elif selected_data == 'title_ratings':
        return title_rating_schema
    elif selected_data == 'title_episode':
        return title_episode_schema
    elif selected_data == 'title_akas':
        return title_akas_schema
    elif selected_data == 'title_crew':
        return title_crew_schema
    else:
        return None

def datasetRevamp(selected_data, df):
    if selected_data == 'name_basics':
        return df.withColumnRenamed('primaryName', 'primary_name') \
            .withColumnRenamed('birthYear', 'birth_year') \
            .withColumnRenamed('deathYear', 'death_year') \
            .withColumnRenamed('primaryProfession', 'primary_profession') \
            .withColumnRenamed('knownForTitles', 'known_for_titles')

    elif selected_data == 'title_basics':
        return df.withColumnRenamed('titleType', 'title_type') \
            .withColumnRenamed('primaryTitle', 'primary_title') \
            .withColumnRenamed('originalTitle', 'original_title') \
            .withColumnRenamed('isAdult', 'is_adult') \
            .withColumnRenamed('startYear', 'start_year') \
            .withColumnRenamed('endYear', 'end_year') \
            .withColumnRenamed('runtimeMinutes', 'runtime_minutes') \
            .withColumn('imdb_link', F.concat(F.lit('https://www.imdb.com/title/'), F.col('tconst'))) \
            .filter(F.col('title_type').isin(['movie', 'tvSeries', 'tvMiniSeries', 'tvMovie', 'video'])) \
            .withColumn('title_type',  F.when(F.col('title_type') == 'movie', 'Movie')
                .when(F.col('title_type') == 'tvSeries', 'TvSeries')
                .when(F.col('title_type') == 'tvMiniSeries', 'TvMiniSeries')
                .when(F.col('title_type') == 'tvMovie', 'TvMovie')
                .when(F.col('title_type') == 'video', 'Video')
                .otherwise(F.col('title_type'))
            )
    elif selected_data == 'title_principals':
        return df.select('tconst', 'ordering', 'nconst', 'category', 'characters') \
            .filter(df['category'].like('act%'))
    elif selected_data == 'title_ratings':
        return df.withColumnRenamed('averageRating', 'average_rating') \
            .withColumnRenamed('numVotes', 'number_of_votes')
    elif selected_data == 'title_episode':
        return df.withColumnRenamed('parentTconst', 'parent_tconst') \
            .withColumnRenamed('seasonNumber', 'season_number') \
            .withColumnRenamed('episodeNumber', 'episode_number')
    elif selected_data == 'title_akas':
        return df.withColumnRenamed('titleId', 'tconst') \
           .withColumnRenamed('isOriginalTitle', 'is_original_title')
    else:
        #this will take care of `title_crew` data
        return df

def replaceSplashNWithNone(df):
    for column in df.columns:
        df = df.withColumn(column, F.when(df[column] == '\\N', None).otherwise(df[column]))

    return df

parser = argparse.ArgumentParser()

parser.add_argument('--src_input', required=True)
parser.add_argument('--selected_data', required=True)
parser.add_argument('--dest_output', required=True)
parser.add_argument('--spark_tmp_bucket', required=True)

args = parser.parse_args()

selected_data = args.selected_data
schema = determineDatasetSchema(selected_data)
if schema is None:
    raise Exception('Unknown selected data', selected_data);

src_input = args.src_input
dest_output = args.dest_output

spark = SparkSession.builder \
        .appName('init-data-transformation') \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', args.spark_tmp_bucket)

dataset_df = spark.read.option('delimiter', '\t') \
    .option('header', 'true') \
    .csv(src_input, schema=schema)


dataset_df = datasetRevamp(selected_data, dataset_df)
dataset_df = replaceSplashNWithNone(dataset_df)

dataset_df.repartition(20).write.parquet(dest_output, mode='overwrite')
