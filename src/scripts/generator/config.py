CONFIG = {
    "POSTGRE_METADATA_USERNAME": "student",
    "POSTGRE_METADATA_PASSWORD": "de-student",
    "POSTGRE_METADATA_SERVER": "rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net",
    "POSTGRE_METADATA_PORT": "6432",
    "POSTGRE_METADATA_DB": "de",
    "POSTGRE_METADATA_TB": "public.subscribers_restaurants",
    "COUNTER_FILE_NAME": "counter.txt",
    "BASH_KAFKA_ODLD": """kafkacat -b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 -X security.protocol=SASL_SSL -X sasl.mechanisms=SCRAM-SHA-512 -X sasl.username="de-student" -X sasl.password="ltcneltyn" -X ssl.ca.location=/home/xeon/data/CA.pem -t student.topic.cohort12.intelxeon_proj_in -K: -P -l /home/xeon/app/data""",
    "SSL_CA_PATH":"/home/xeon/data/CA.pem",
    "DATA_PATH":"/home/xeon/sprint8/de-project-sprint-8/data",
    "GEN_PATH":"/home/xeon/sprint8/de-project-sprint-8"

}
CONFIG['BASH_KAFKA'] = f"""kafkacat \
-b rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091 \
-X security.protocol=SASL_SSL \
-X sasl.mechanisms=SCRAM-SHA-512 \
-X sasl.username="de-student" \
-X sasl.password="ltcneltyn" \
-X ssl.ca.location={CONFIG['SSL_CA_PATH']} \
-t student.topic.cohort12.intelxeon_proj_in \
-K: -P \
-l {CONFIG['DATA_PATH']}/data"""
