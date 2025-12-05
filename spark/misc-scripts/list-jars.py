"""
list-jars.py – print every JAR Spark knows about in local mode.

Run:
    spark-submit --master local[2] --jars extra1.jar,extra2.jar list-jars.py
"""
from pyspark.sql import SparkSession

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("JarInspector")
        .getOrCreate()
    )
    sc   = spark.sparkContext
    jvm  = sc._jvm

    # JARs Spark *would distribute* in cluster mode
    print("=== JARs SparkContext would ship (listJars) ===")
    shipped = sc._jsc.sc().listJars()           # Scala Seq[String]
    for i in range(shipped.size()):
        print("  shipped ->", shipped.apply(i))
    if shipped.isEmpty():
        print("  (none – expected in local mode)")

    # JARs specified via --jars / spark.jars
    print("\n=== spark.jars configuration ===")
    conf_jars = spark.conf.get("spark.jars", "")
    for jar in filter(None, conf_jars.split(",")):
        print("  spark.jars ->", jar)
    if not conf_jars:
        print("  (spark.jars not set)")

    # Entire JVM class-path (includes $SPARK_HOME/jars and conf_jars)
    print("\n=== JVM class-path entries ===")
    cp      = jvm.java.lang.System.getProperty("java.class.path")
    sep     = jvm.java.lang.System.getProperty("path.separator")
    for entry in cp.split(sep):
        print("  classpath ->", entry)

    spark.stop()

if __name__ == "__main__":
    main()