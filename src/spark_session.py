"""
spark_session.py
-----------------
Provides a single, reusable, local-only SparkSession factory.
"""

import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

# Set Hadoop location before Spark starts
os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["hadoop.home.dir"] = r"D:\hadoop"

# Allow running this file directly as well as importing it as a module
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config import config


def get_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", config.SPARK_SHUFFLE_PARTITIONS)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    if spark is not None:
        spark.stop()