from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tpch_examples.config.ConfigStore import *
from tpch_examples.udfs.UDFs import *

def tpch_nation(spark: SparkSession) -> DataFrame:
    return spark.read.table("samples.tpch.nation")
