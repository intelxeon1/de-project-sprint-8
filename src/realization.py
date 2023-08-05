from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType,LongType
from pyspark.sql import DataFrame
import time
import sys, os

sys.path.append(os.getcwd())
from config import kafka_security_options, kafka_topic_out, kafka_topic_in,marketing_table,postgresql_settings,output_table,spark_jars_packages




schema = StructType([
    StructField("restaurant_id",StringType()),
    StructField("adv_campaign_id",StringType()),
    StructField("adv_campaign_content",StringType()),
    StructField("adv_campaign_owner",StringType()),
    StructField("adv_campaign_owner_contact",StringType()),
    StructField("adv_campaign_datetime_start",LongType()),
    StructField("adv_campaign_datetime_end",LongType()),
    StructField("datetime_created",LongType()),
])

def spark_init(name) -> SparkSession:
    spark = SparkSession.builder \
        .appName(name) \
        .config("spark.jars.packages",spark_jars_packages) \
        .getOrCreate()
    return  spark   



def transform(df: DataFrame) -> DataFrame:
    current_time = int(time.time())
    return df.selectExpr("Cast(key as STRING)","CAST(value as STRING)",'timestamp') \
            .withColumn('json',F.from_json(F.col('value'),schema)) \
            .selectExpr('key','json.*','timestamp') \
            .dropDuplicates(['key','timestamp']).withWatermark('timestamp','10 minutes') \
            .filter(F.col('adv_campaign_datetime_end') > current_time )

def read_marketing(spark: SparkSession) -> DataFrame:
    df = spark.read.format("jdbc").options(**postgresql_settings).options(**marketing_table).load().selectExpr("restaurant_id", "client_id").distinct()
    return df


def read_campaing_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream.format("kafka") \
        .options(**kafka_security_options) \
        .option('subscribe',kafka_topic_in) \
        .load()
    transform_df = transform(df)
    return transform_df


def join(campaing_df, marketing_df) -> DataFrame:
    joined = campaing_df.join(marketing_df,how='left',on=['restaurant_id']) \

    return joined

def foreach_batch_function(df:DataFrame, epoch_id):    
    saved_df = df.cache()
    trigger_datetime_created = int(time.time())
    pg_df = saved_df.withColumns({'trigger_datetime_created':F.lit(trigger_datetime_created),'feedback':F.lit('')}) \
            .drop('timestamp','key') \
            .withColumnRenamed('restaurant_id','restaraunt_id')
    pg_df.write.format("jdbc").mode('append').options(**postgresql_settings).options(**output_table).save()    
    tmp_df = pg_df.drop('feedback')
    k_df = tmp_df.withColumn('value',F.to_json(F.struct([tmp_df[x] for x in tmp_df.columns ]))).select('value').withColumn('key',F.lit(''))
    k_df.write.option('header',True).mode('append').csv("/home/xeon/sprint8/de-project-sprint-8/output")
    k_df.show(truncate=False)
    k_df.write.format('kafka').options(**kafka_security_options).option('topic',kafka_topic_out).save()
    
    



if __name__ == "__main__":
    spark = spark_init('Project 8')
    campaing_stream = read_campaing_stream(spark)
    marketing_df = read_marketing(spark)
    
    result = join(campaing_stream, marketing_df)

    query = (result.writeStream \
                .foreachBatch(foreach_batch_function) \
                .start()
            )
    query.awaitTermination()