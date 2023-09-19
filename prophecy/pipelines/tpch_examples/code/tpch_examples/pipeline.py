from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from tpch_examples.config.ConfigStore import *
from tpch_examples.udfs.UDFs import *
from prophecy.utils import *
from tpch_examples.graph import *

def pipeline(spark: SparkSession) -> None:
    df_tpch_nation = tpch_nation(spark)
    df_tpch_customer = tpch_customer(spark)
    df_JoinNation = JoinNation(spark, df_tpch_customer, df_tpch_nation)
    df_num_customers_by_nation_name = num_customers_by_nation_name(spark, df_JoinNation)
    country_customer_report(spark, df_num_customers_by_nation_name)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/tpch_examples")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/tpch_examples", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/tpch_examples")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
