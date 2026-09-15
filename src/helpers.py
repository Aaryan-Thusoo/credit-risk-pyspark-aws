def build_spark(app_name: str, use_s3_packages: bool):
    try:
        from pyspark.sql import SparkSession
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError(
            "PySpark is not installed. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    builder = SparkSession.builder.appName(app_name)
    if use_s3_packages:
        builder = (
            builder.config(
                "spark.jars.packages",
                ",".join([
                    "org.apache.hadoop:hadoop-aws:3.3.4",
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
                ]),
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
            )
        )
    return builder.getOrCreate()
