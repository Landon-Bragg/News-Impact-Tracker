import os
from pyspark.sql import SparkSession

# Minimal Windows-safe config
os.environ["HADOOP_HOME"]      = r"C:\hadoop"
os.environ["hadoop.home.dir"]  = r"C:\hadoop"
os.environ["PATH"]             = r"C:\hadoop\bin;" + os.environ.get("PATH","")
os.environ["JAVA_TOOL_OPTIONS"]= r"-Djava.io.tmpdir=C:\spark-tmp"

spark = (SparkSession.builder
    .appName("spark-probe")
    .config("spark.master", "local[*]")
    .config("spark.local.dir", r"C:\spark-tmp")
    .config("spark.sql.warehouse.dir", "file:///C:/spark-warehouse")
    .config("spark.hadoop.tmp.dir",     "file:///C:/spark-tmp")
    # Windows raw local FS (avoids NativeIO edge cases)
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("spark.driver.extraJavaOptions",
            "-Djava.library.path=C:\\hadoop\\bin -Dhadoop.home.dir=C:\\hadoop -Djava.io.tmpdir=C:\\spark-tmp")
    .config("spark.executor.extraJavaOptions",
            "-Djava.library.path=C:\\hadoop\\bin -Dhadoop.home.dir=C:\\hadoop -Djava.io.tmpdir=C:\\spark-tmp")
    .getOrCreate())

print("Spark version:", spark.version)
spark.range(1).show()
spark.stop()
