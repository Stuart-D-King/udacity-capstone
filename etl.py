import os
import pandas as pd
import numpy as np
import datetime
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from collections import Counter
from functools import reduce


def calc_median(lst):
    '''
    Calculate median from list of values

    INPUT
    lst: list of values

    OUTPUT
    Calculated median
    '''
    med = np.median(lst)
    return float(med)


int_counter_udf = f.udf(lambda s: dict(Counter(s)), t.MapType(t.IntegerType(), t.IntegerType()))
str_counter_udf = f.udf(lambda s: dict(Counter(s)), t.MapType(t.StringType(), t.IntegerType()))
median_func = f.udf(calc_median, t.DoubleType())


def create_spark_session():
    '''
    Create and return a SparkSession object
    '''
    spark = SparkSession \
        .builder \
        .config('spark.jars.packages', 'saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.3,com.amazonaws:aws-java-sdk-pom:1.10.34') \
        .enableHiveSupport() \
        .getOrCreate()
    return spark


def process_immigration_data(spark, bucket, key=None, secret=None):
    '''
    Read in immigration data, clean and aggregrate into an analytical table, and save output to S3

    INPUT
    spark: SparkSession object
    bucket: S3 bucket name
    key: AWS Access ID (default=None)
    secret: AWS Secret Assess Key (default=None)

    OUTPUT
    none
    '''
    def _convert_date(d):
        '''
        Convert SAS date to datetime object

        INPUT
        d: number of days

        OUTPUT
        Datetime object
        '''
        epoch = datetime.datetime(1960, 1, 1)
        if d is not None:
            return epoch + datetime.timedelta(days=d)
        else:
            return None

    def _map_trip_purpose(tp):
        '''
        Map trip purpose codes to strings

        INPUT
        tp: integer identifier of trip purpose

        OUTPUT
        String identifier of trip purpose
        '''
        if tp is not None:
            if tp == 1:
                return 'Business'
            elif tp == 2:
                return 'Pleasure'
            elif tp == 3:
                return 'Student'
        else:
            None

    # UDFs for immigration table
    date_func = f.udf(lambda d: _convert_date(d), t.DateType())
    trip_purpose_func = f.udf(lambda tp: _map_trip_purpose(tp), t.StringType())

    # read in all SAS files into one dataframe
    # sas_filepaths = get_filepaths(input_data)
    # frames = []
    # for f in sas_filepaths:
    #     df = spark.read.format('com.github.saurfang.sas.spark') \
    #         .load('../..' + f.split('/')[-1]) \
    #         .select('i94mon', 'i94cit', 'i94port', 'arrdate', 'depdate',
    #         'i94mode', 'i94bir', 'i94visa', 'gender', 'airline', 'visatype')
    #     frames.append(df)

    # df_imm = reduce(DataFrame.union, frames)

    # shouldn't need key and secret if run on EMR
    s3 = boto3.resource('s3',
        region_name='us-west-2')

    capstoneBucket = s3.Bucket(bucket)

    frames = []
    for obj in capstoneBucket.objects.filter(Prefix='data/sas_data'):
        df = spark.read.format('com.github.saurfang.sas.spark') \
                .load('s3n://' + bucket + '/' + obj.key) \
                .select('i94mon', 'i94cit', 'i94port', 'arrdate', 'depdate', 'i94mode', 'i94bir', 'i94visa', 'gender', 'airline', 'visatype')

        frames.append(df)

    df_imm = reduce(DataFrame.union, frames)
    df_imm.persist()

    imm_table = df_imm \
        .filter(f.col('i94mode') == 1) \
        .filter(~f.col('i94port').isin(['XXX', '888', 'UNK'])) \
        .select(
            f.col('i94mon').cast(t.IntegerType()).alias('month'),
            f.col('i94cit').cast(t.IntegerType()).alias('country_id'),
            f.col('i94port').alias('airport_cd'),
            f.col('arrdate').cast(t.IntegerType()).alias('arrival_date'),
            f.col('depdate').cast(t.IntegerType()).alias('departure_date'),
            f.col('i94bir').cast(t.IntegerType()).alias('age'),
            f.col('i94visa').cast(t.IntegerType()).alias('trip_purpose'),
            f.col('gender'),
            f.col('airline').alias('airline_cd'),
            f.col('visatype').alias('visa_type')) \
        .dropDuplicates() \
        .withColumn('arrival_date', date_func(f.col('arrival_date'))) \
        .withColumn('departure_date', date_func(f.col('departure_date'))) \
        .withColumn('trip_purpose', trip_purpose_func(f.col('trip_purpose'))) \
        .withColumn('length_of_stay', f.datediff(f.col('departure_date'), f.col('arrival_date'))) \
        .withColumn('gender', f.when(f.col('gender') == 'X', None).otherwise(f.col('gender'))) \
        .drop('arrival_date', 'departure_date')

    # GENDER counts by month and airport
    gender_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.collect_list('gender').alias('gender')) \
        .withColumn('gender', str_counter_udf(f.col('gender'))) \
        .alias('gender_table')

    # AVG LENGTH OF STAY by month and airport
    stay_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.round(f.mean('length_of_stay'),2).alias('avg_stay')) \
        .alias('stay_table')

    # MEDIAN AGE by month and airport
    age_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(median_func(f.collect_list(f.col('age'))).alias('median_age')) \
        .alias('age_table')

    # TRIP PURPOSE countes by month and airport
    trip_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.collect_list('trip_purpose').alias('trip_purpose')) \
        .withColumn('trip_purpose', str_counter_udf(f.col('trip_purpose'))) \
        .alias('trip_table')

    # VISA TYPE counts by month and airport
    visa_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.collect_list('visa_type').alias('visa_type')) \
        .withColumn('visa_type', str_counter_udf(f.col('visa_type'))) \
        .alias('visa_table')

    # COUNTRY counts by month and airport
    country_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.collect_list('country_id').alias('countries')) \
        .withColumn('countries', int_counter_udf(f.col('countries'))) \
        .alias('country_table')

    # AIRLINE counts by month and airport
    airline_table = imm_table.groupBy('month', 'airport_cd') \
        .agg(f.collect_list('airline_cd').alias('airlines')) \
        .withColumn('airlines', str_counter_udf(f.col('airlines'))) \
        .alias('airline_table')

    # TOTAL count by month and airport
    cnt_table = imm_table.groupBy('month', 'airport_cd').count().alias('cnt_table')

    # full immigration table
    immigration_table = cnt_table \
        .join(gender_table, on=[
            f.col('cnt_table.month') == f.col('gender_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('gender_table.airport_cd')], how='left') \
        .join(stay_table, on=[
            f.col('cnt_table.month') == f.col('stay_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('stay_table.airport_cd')], how='left') \
        .join(age_table, on=[
            f.col('cnt_table.month') == f.col('age_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('age_table.airport_cd')], how='left') \
        .join(trip_table, on=[
            f.col('cnt_table.month') == f.col('trip_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('trip_table.airport_cd')], how='left') \
        .join(visa_table, on=[
            f.col('cnt_table.month') == f.col('visa_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('visa_table.airport_cd')], how='left') \
        .join(country_table, on=[
            f.col('cnt_table.month') == f.col('country_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('country_table.airport_cd')], how='left') \
        .join(airline_table, on=[
            f.col('cnt_table.month') == f.col('airline_table.month'), \
            f.col('cnt_table.airport_cd') == f.col('airline_table.airport_cd')], how='left') \
        .select(
            cnt_table.month,
            cnt_table.airport_cd,
            gender_table.gender,
            stay_table.avg_stay,
            age_table.median_age,
            trip_table.trip_purpose,
            visa_table.visa_type,
            country_table.countries,
            airline_table.airlines) \
        .withColumn('immigration_id', f.monotonically_increasing_id())

    # write immigration table to parquet files
    imm_output = 's3n://' + bucket + '/parquet-files/immigration'
    immigration_table.write.partitionBy('month','airport_cd').parquet(imm_output)


def process_airport_data(spark, bucket):
    '''
    Read in airport data, clean the data, and save dimensional table to S3

    INPUT
    spark: SparkSession object
    bucket: S3 bucket name

    OUTPUT
    none
    '''
    input_data = 's3n://' + bucket + '/data/airport-codes_csv.csv'
    df_air = spark.read.csv(input_data, header=True)
    df_air.persist()

    airport_table = df_air.filter(f.col('iso_country') == 'US') \
        .where(f.col('type').like('%airport%')) \
        .withColumn('coord_split', f.split(f.col('coordinates'), ',')) \
        .withColumn('state_split', f.split(f.col('iso_region'), '-')) \
        .select(
            f.col('iata_code').alias('airport_cd'),
            f.col('type'),
            f.col('name'),
            f.col('state_split')[1].alias('state'),
            f.col('municipality').alias('city'),
            f.col('coord_split')[1].cast(t.DoubleType()).alias('latitude'),
            f.col('coord_split')[0].cast(t.DoubleType()).alias('longitude')) \
        .dropDuplicates()

    # write airport table to parquet files
    airport_output = 's3n://' + bucket + '/parquet-files/airports'
    airport_table.write.parquet(airport_output)


def process_demographics_data(spark, bucket):
    '''
    Read in city demographics data, clean the data, and save dimensional table to S3

    INPUT
    spark: SparkSession object
    bucket: S3 bucket name

    OUTPUT
    none
    '''
    input_data = 's3n://' + bucket + '/data/us-cities-demographics.csv'
    df_demo = spark.read.csv(input_data, header=True, sep=';')
    df_demo.persist()

    df_city = df_demo.select(
            f.col('City').alias('city'),
            f.col('State Code').alias('state'),
            f.col('Median Age').cast(t.DoubleType()).alias('median_age'),
            f.col('Male Population').cast(t.IntegerType()).alias('male_pop'),
            f.col('Female Population').cast(t.IntegerType()).alias('female_pop'),
            f.col('Total Population').cast(t.IntegerType()).alias('total_pop'),
            f.col('Number of Veterans').cast(t.IntegerType()).alias('veterans'),
            f.col('Foreign-born').cast(t.IntegerType()).alias('foreign_born'),
            f.col('Average Household Size').cast(t.DoubleType()).alias('avg_hh_size')) \
        .dropDuplicates()

    df_city_pivot = df_demo.groupBy('City', 'State Code') \
        .pivot('Race').agg({'Count': 'sum'}) \
        .withColumnRenamed('City', 'city') \
        .withColumnRenamed('State Code', 'state') \
        .withColumnRenamed('American Indian and Alaska Native', 'native_am_pop') \
        .withColumnRenamed('Asian', 'asian_pop') \
        .withColumnRenamed('Black or African-American', 'black_pop') \
        .withColumnRenamed('Hispanic or Latino', 'hispanic_pop') \
        .withColumnRenamed('White', 'white_pop')

    demo_table = df_city.join(df_city_pivot, on=[ \
            df_city.city == df_city_pivot.city, df_city.state == df_city_pivot.state]) \
        .select(df_city.city,
            df_city.state,
            df_city.median_age,
            df_city.male_pop,
            df_city.female_pop,
            df_city_pivot.native_am_pop.cast(t.IntegerType()),
            df_city_pivot.asian_pop.cast(t.IntegerType()),
            df_city_pivot.black_pop.cast(t.IntegerType()),
            df_city_pivot.hispanic_pop.cast(t.IntegerType()),
            df_city_pivot.white_pop.cast(t.IntegerType()),
            df_city.veterans,
            df_city.foreign_born,
            df_city.avg_hh_size)

    # write demographics table to parquet files
    demo_output = 's3n://' + bucket + '/parquet-files/demographics'
    demo_table.write.partitionBy('city','state').parquet(demo_output)


def process_temperature_data(spark, bucket):
    '''
    Read in land temperatures data, clean and process the data into temperature and time tables, and save to S3

    INPUT
    spark: SparkSession object
    bucket: S3 bucket name

    OUTPUT
    none
    '''
    def _cel_to_fah(c):
        '''
        Convert celsius to fahrenheit

        INPUT
        c: integer value for temperature in Celsius

        OUTPUT
        Float value for temperature in Fahrenheit
        '''
        if c is not None:
            return c*(9/5)+32
        else:
            return None

    def _convert_latlong(c):
        '''
        Convert cardinal directions to +/- latitude and longitude coordinates

        INPUT
        c: latitude or longitude coordinate with cardinal direction as the last character

        OUTPUT
        Postive or negative coordinate value
        '''
        if c is not None:
            num = c[:-1]
            quad = c[-1]
            if quad.upper() in ('S', 'W'):
                return float(num) * -1
            elif quad.upper() in ('N', 'E'):
                return float(num)
            else:
                None
        else:
            return None

    temp_conver_func = f.udf(lambda c: _cel_to_fah(c), t.DoubleType())
    latlong_func = f.udf(lambda c: _convert_latlong(c), t.DoubleType())

    input_data = 's3n://' + bucket + '/data/GlobalLandTemperaturesByCity.csv'
    df_temp = spark.read.csv(input_data, header=True)
    df_temp.persist()

    temp_table = df_temp.filter(f.col('Country') == 'United States') \
        .withColumn('dim_date', f.to_date(f.col('dt'))) \
        .filter(f.year(f.col('dim_date')) >= 1950) \
        .na.drop(subset=['AverageTemperature']) \
        .select(
            f.month(f.col('dim_date')).alias('month'),
            f.col('AverageTemperature').cast(t.DoubleType()).alias('avg_temp'),
            f.col('City').alias('city'),
            f.col('Latitude').alias('latitude'),
            f.col('Longitude').alias('longitude')) \
        .withColumn('avg_temp', f.round(temp_conver_func(f.col('avg_temp')), 2)) \
        .withColumn('latitude', latlong_func(f.col('latitude'))) \
        .withColumn('longitude', latlong_func(f.col('longitude'))) \
        .dropDuplicates() \
        .groupBy('month', 'city', 'latitude', 'longitude') \
        .agg(median_func(f.collect_list(f.col('avg_temp'))).alias('median_avg_temp'))

    # write temperature table to parquet files
    temp_output = 's3n://' + bucket + '/parquet-files/temperature'
    temp_table.write.partitionBy('month','city').parquet(temp_output)


def main():
    '''
    Run ETL process by instantiating a SparkSession object, creating dimension and fact tables, and saving output to S3
    '''
    bucket = 'sking-udacity-capstone'
    spark = create_spark_session()

    process_immigration_data(spark, bucket) # shouldn't need key and secret if run on EMR
    process_airport_data(spark, bucket)
    process_demographics_data(spark, bucket)
    process_temperature_data(spark, bucket)

    spark.stop()


if __name__ == '__main__':
    main()
