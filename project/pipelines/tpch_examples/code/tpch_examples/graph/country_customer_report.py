from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tpch_examples.config.ConfigStore import *
from tpch_examples.udfs.UDFs import *

def country_customer_report(spark: SparkSession, in0: DataFrame):
    in0.write.format("delta").mode("overwrite").saveAsTable("default.country-customer-report")
