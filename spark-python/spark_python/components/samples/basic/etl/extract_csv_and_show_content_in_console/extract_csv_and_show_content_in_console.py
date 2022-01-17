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

# spark-conf
spark_conf = SparkConf().setAll(spark_conf_parameters)

# spark-session
spark_session = SparkSession.builder.config(conf=spark_conf).getOrCreate()

# spark-context
spark_context = spark_session.sparkContext
# spark-context-set-level
spark_context.setLogLevel(config["log-level"])

# spark-df
spark_df = spark_session.read.csv(f"{os.path.join(os.getcwd(), config['file-path'])}")

# show-content
spark_df.show(10)
