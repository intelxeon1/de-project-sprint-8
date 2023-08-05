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