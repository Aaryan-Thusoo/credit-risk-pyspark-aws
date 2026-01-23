from ingest_performance import build_spark

spark = build_spark("check-schema", use_s3_packages=True)
df = spark.read.parquet("s3a://credit-risk-ews-data/cleaned/freddie_mac/performance/v1/")
df.printSchema()
spark.stop()
