from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tpch_examples.config.ConfigStore import *
from tpch_examples.udfs.UDFs import *

def num_customers_by_nation_name(spark: SparkSession, in0: DataFrame) -> DataFrame:
    df1 = in0.groupBy(col("n_name").alias("nation_name"))

    return df1.agg(countDistinct(col("c_custkey")).alias("num_customers"))
