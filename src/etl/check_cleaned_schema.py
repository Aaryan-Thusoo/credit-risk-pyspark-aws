import argparse
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from helpers import build_spark


def main():
    parser = argparse.ArgumentParser(description="Print the schema for a cleaned parquet dataset.")
    parser.add_argument("path", help="Parquet path to inspect, e.g. s3a://bucket/cleaned/freddie_mac/performance/v1/")
    parser.add_argument("--use_s3_packages", action="store_true", help="Enable if reading s3a:// fails locally.")
    args = parser.parse_args()

    spark = build_spark("check-schema", use_s3_packages=args.use_s3_packages)
    df = spark.read.parquet(args.path)
    df.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
