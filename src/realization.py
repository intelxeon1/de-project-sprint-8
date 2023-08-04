from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType,LongType
from pyspark.sql import DataFrame
import time


spark_jars_packages = ",".join(
    [
        "org.postgresql:postgresql:42.6.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        #"org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1"
    ]
)

# настройки security для кафки
kafka_security_options = {
    'kafka.bootstrap.servers':'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
    'kafka.security.protocol':'SASL_SSL',
    'kafka.sasl.mechanism':'SCRAM-SHA-512',
    'kafka.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',    
    #'kafka.ssl.truststore.location':'/home/xeon/data/cacerts',
    #'kafka.ssl.truststore.password':'Gold2222'
}

kafka_topic_in = 'student.topic.cohort12.intelxeon_proj_in'
kafka_topic_out = 'student.topic.cohort12.intelxeon_proj_out'

postgresql_settings = {
    "url": "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de",
    "driver": "org.postgresql.Driver",
    "user": "student",
    "password": "de-student"
}
marketing_table = {  "dbtable": "public.subscribers_restaurants"}
output_table = {'dbtable':'public.subscribers_feedback_4'}

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
    spark = spark_init('join stream')
    campaing_stream = read_campaing_stream(spark)
    marketing_df = read_marketing(spark)
    
    result = join(campaing_stream, marketing_df)

    query = (result.writeStream \
                .foreachBatch(foreach_batch_function) \
                .start()
            )
    query.awaitTermination()