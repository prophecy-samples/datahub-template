from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from tpch_examples.config.ConfigStore import *
from tpch_examples.udfs.UDFs import *

def JoinNation(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0\
        .alias("in0")\
        .join(in1.alias("in1"), (col("in0.c_nationkey") == col("in1.n_nationkey")), "inner")\
        .select(col("in0.c_custkey").alias("c_custkey"), col("in0.c_name").alias("c_name"), col("in0.c_address").alias("c_address"), col("in0.c_nationkey").alias("c_nationkey"), col("in0.c_phone").alias("c_phone"), col("in0.c_acctbal").alias("c_acctbal"), col("in0.c_mktsegment").alias("c_mktsegment"), col("in0.c_comment").alias("c_comment"), col("in1.n_name").alias("n_name"))
