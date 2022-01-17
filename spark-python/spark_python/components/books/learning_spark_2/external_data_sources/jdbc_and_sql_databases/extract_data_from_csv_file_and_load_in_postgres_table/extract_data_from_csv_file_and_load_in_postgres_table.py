import os.path

from pyspark.conf import SparkConf
from pyspark.sql import SparkSession

import kimera_data.components.etl as kimera_etl

# kimera-data
config = kimera_etl \
    .extractor() \
    .engine('raw') \
    .format('json') \
    .option("fp_or_sj", f"{os.path.join(os.getcwd(), 'config.json')}") \
    .read()

# spark-conf-parameters
spark_conf_parameters = [(k, v) for k, v in config['spark-config'].items()]
spark_jdbc_parameters = config['jdbc']

# spark-conf
spark_conf = SparkConf().setAll(spark_conf_parameters)

# spark-session
spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# spark-context
spark_context = spark_session.sparkContext
# spark-context-set-level
spark_context.setLogLevel("INFO")

# spark-df-postgres
df_postgres_covid_variants = spark_session.read.jdbc(
    spark_jdbc_parameters["url"],
    spark_jdbc_parameters["table"],
    properties=spark_jdbc_parameters["properties"]
)

# add schema to csv file from postgres table schema
config["extractors-config"]["csv"]["schema"] = df_postgres_covid_variants.schema

df_csv_covid_variants = spark_session.read.csv(**config["extractors-config"]["csv"]["covid-variants"])

# show-content
df_postgres_covid_variants.show(10)

# csv load to postgres
df_csv_covid_variants.write.jdbc(
    spark_jdbc_parameters["url"],
    spark_jdbc_parameters["table"],
    spark_jdbc_parameters["mode"],
    properties=spark_jdbc_parameters["properties"]
)
