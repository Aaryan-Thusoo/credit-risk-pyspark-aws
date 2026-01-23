def main():
    parser = argparse.ArgumentParser(
        description="Ingest Freddie Mac servicing sample files from S3 into partitioned Parquet."
    )
    parser.add_argument("--input", required=True, help="S3A prefix for raw txt files, e.g. s3a://bucket/raw/.../performance/")
    parser.add_argument("--output", required=True, help="S3A prefix for cleaned parquet output, e.g. s3a://bucket/cleaned/.../performance/")
    parser.add_argument("--use_s3_packages", action="store_true", help="Enable if you get s3a filesystem/jar errors locally.")
    args = parser.parse_args()

    spark = build_spark("freddie-mac-ingest-performance", use_s3_packages=args.use_s3_packages)

    # ✅ Read ONLY .txt files under the prefix (prevents Spark from trying to read zips/other objects)
    input_path = args.input.rstrip("/") + "/*.parquet"

    df = (
        spark.read
        .option("sep", "|")
        .option("header", "false")
        .option("mode", "PERMISSIVE")
        .parquet(input_path)
        .withColumn("_source_file", F.input_file_name())
    )

    # ✅ Guardrail: if parsing failed and you only got one column, stop before writing bad Parquet
    if len(df.columns) <= 1:
        df.show(5, truncate=False)
        raise RuntimeError(
            f"Parsed only {len(df.columns)} column(s). "
            f"This usually means the delimiter wasn't applied or input included non-text files. "
            f"Input path used: {input_path}"
        )

    # Derive partition columns from the in-file reporting period (YYYYMM)
    df = (
        df.withColumn("yyyymm", F.col("_c1"))
          .filter(F.col("yyyymm").rlike(r"^\d{6}$"))
          .withColumn("year", F.substring(F.col("yyyymm"), 1, 4).cast("int"))
          .withColumn("month", F.substring(F.col("yyyymm"), 5, 2).cast("int"))
    )

    df = df.drop("_source_file")

    (
        df.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output)
    )

    print(f"Wrote Parquet to: {args.output}")
    spark.stop()

if __name__ == "__main__":
