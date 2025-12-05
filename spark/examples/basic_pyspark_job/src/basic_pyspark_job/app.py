"""
Example of a basic PySpark job without Spark Connect or JDBC/Thrift.
"""
from pyspark.sql import SparkSession

def test_dataframe_operations(spark: SparkSession) -> None:
    """
    Tests basic DataFrame operations.
    """
    print("Running DataFrame operations test...")
    # Create a simple DataFrame
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)

    print("Initial DataFrame:")
    df.show()

    # Perform a simple transformation (filter)
    filtered_df = df.filter(df.id > 1)

    print("Filtered DataFrame (id > 1):")
    filtered_df.show()

    # Perform an action (count)
    count = filtered_df.count()
    print(f"Count of rows in filtered DataFrame: {count}")
    print("DataFrame operations test completed.")

def test_spark_sql(spark: SparkSession) -> None:
    """
    Tests basic Spark SQL capabilities.
    """
    print("Running Spark SQL test...")
    # Create a simple DataFrame
    data = [("David", 4), ("Eve", 5), ("Frank", 6)]
    columns = ["name", "id"]
    df = spark.createDataFrame(data, columns)

    # Register the DataFrame as a temporary view
    df.createOrReplaceTempView("people")

    # Run a SQL query
    sql_df = spark.sql("SELECT name, id FROM people WHERE id < 6 ORDER BY id DESC")

    print("Result of SQL query 'SELECT name, id FROM people WHERE id < 6 ORDER BY id DESC':")
    sql_df.show()

    count_sql = spark.sql("SELECT COUNT(*) as count FROM people WHERE id < 6").collect()[0]['count']
    print(f"Count of rows from SQL query: {count_sql}")
    print("Spark SQL test completed.")

def main() -> None:
    # Initialize Spark Session
    spark: SparkSession = SparkSession.builder \
        .appName("BasicPySparkJob") \
        .getOrCreate()

    print("Spark Session Initialized successfully.")

    test_dataframe_operations(spark)
    print("-" * 30) # Separator
    test_spark_sql(spark)

    # Stop the Spark Session
    spark.stop()
    print("Spark Session stopped.")

if __name__ == "__main__":
    main()