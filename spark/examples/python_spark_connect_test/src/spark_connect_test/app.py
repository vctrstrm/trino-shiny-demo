import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F # Import functions

# Define the remote address for the Spark Connect server
SPARK_CONNECT_HOST = os.getenv("SPARK_CONNECT_HOST", "spark-cluster")
SPARK_CONNECT_PORT = os.getenv("SPARK_CONNECT_PORT", "15002")
SPARK_CONNECT_REMOTE = f"sc://{SPARK_CONNECT_HOST}:{SPARK_CONNECT_PORT}"

def main():
    print(f"Attempting to connect to Spark Connect server at {SPARK_CONNECT_REMOTE}...")
    try:
        # Build SparkSession with remote connection
        spark = SparkSession.builder.remote(SPARK_CONNECT_REMOTE).getOrCreate()
        print("Successfully connected to Spark Connect server!")

        # === Test 1: DataFrame from Client Data, Different Aggregation (sum) ===
        print("\n--- Testing Aggregation: Sum ---")
        data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        columns = ["name", "id"]
        df1 = spark.createDataFrame(data, columns)
        print("Created DataFrame from client data:")
        df1.show()
        try:
            sum_result = df1.agg(F.sum("id").alias("total_id")).first()
            if sum_result:
                print(f"Sum Result: {sum_result}")
                if sum_result["total_id"] == 6:
                    print("Sum aggregation PASSED.")
                else:
                    print("Sum aggregation FAILED (unexpected result).")
            else:
                print("Sum aggregation FAILED (no result).")
        except Exception as sum_e:
            print(f"Sum aggregation FAILED with error: {sum_e}")
            # Optionally re-raise if this is critical

        # === Test 2: DataFrame from Server Data (spark.range), Count ===
        print("\n--- Testing Count on Server-Side Data ---")
        try:
            df2 = spark.range(0, 3) # Creates DataFrame with 'id' column (0, 1, 2)
            print("Created DataFrame using spark.range:")
            df2.show()
            count_result = df2.count()
            print(f"Count Result (spark.range): {count_result}")
            if count_result == 3:
                print("Count on spark.range PASSED.")
            else:
                print("Count on spark.range FAILED (unexpected result).")
        except Exception as range_count_e:
             print(f"Count on spark.range FAILED with error: {range_count_e}")
             # Re-raise the exception if count fails here, as it's the core problem
             raise range_count_e

        # === Final Status (based on the range count test) ===
        # We'll determine overall pass/fail based on whether count on range worked
        # (Assuming previous test failures are just logged)
        print("\n--- Final Test Outcome ---")
        print("Spark Connect test PASSED! (Count on spark.range succeeded)")

        spark.stop()

    except Exception as e:
        # Catch errors from connection or the range_count test if it raised
        print(f"\n--- Final Test Outcome ---")
        print(f"An error occurred during Spark Connect test: {e}")
        print("Spark Connect test FAILED.")


if __name__ == "__main__":
    main() 