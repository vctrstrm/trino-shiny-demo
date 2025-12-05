package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

public class ThriftJdbcTest {

    private static final String HIVE_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    // Allow overriding host via environment variable
    private static final String DEFAULT_SPARK_THRIFT_HOST = "localhost";
    private static final String SPARK_THRIFT_HOST = System.getenv("SPARK_THRIFT_HOST") != null ? System.getenv("SPARK_THRIFT_HOST") : DEFAULT_SPARK_THRIFT_HOST;
    private static final String CONNECTION_URL = "jdbc:hive2://" + SPARK_THRIFT_HOST + ":10000/default";
    private static final String USER = "user"; // Username is often not strictly checked in default Spark Thrift server setup
    private static final String PASSWORD = "password"; // Password is often not strictly checked

    public static void main(String[] args) {
        System.out.println("Attempting to connect to Spark Thrift JDBC server at " + CONNECTION_URL);

        try {
            // Load the Hive JDBC driver
            Class.forName(HIVE_DRIVER_NAME);
        } catch (ClassNotFoundException e) {
            System.err.println("Failed to load Hive JDBC driver: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Establish connection
            // For a default standalone Spark Thrift server, username/password are often ignored
            // but the driver might require non-null values.
            conn = DriverManager.getConnection(CONNECTION_URL, USER, PASSWORD);
            System.out.println("Successfully connected to Spark Thrift JDBC server!");

            stmt = conn.createStatement();

            // Test 1: Show databases
            String sqlShowDb = "SHOW DATABASES";
            System.out.println("Executing query: " + sqlShowDb);
            rs = stmt.executeQuery(sqlShowDb);
            boolean foundDefaultDb = false;
            while (rs.next()) {
                String dbName = rs.getString(1);
                System.out.println("Database: " + dbName);
                if ("default".equalsIgnoreCase(dbName)) {
                    foundDefaultDb = true;
                }
            }
            rs.close(); // Close ResultSet immediately after use

            if (foundDefaultDb) {
                System.out.println("SHOW DATABASES test PASSED (found 'default' database).");
            } else {
                System.out.println("SHOW DATABASES test FAILED (did not find 'default' database).");
            }


            // Test 2: Simple select query
            String sqlSelect = "SELECT 1 + 1 AS result";
            System.out.println("Executing query: " + sqlSelect);
            rs = stmt.executeQuery(sqlSelect);

            int resultValue = -1;
            if (rs.next()) {
                resultValue = rs.getInt("result");
                System.out.println("Query result: " + resultValue);
            }
            rs.close();

            if (resultValue == 2) {
                System.out.println("SELECT 1 + 1 test PASSED!");
            } else {
                System.out.println("SELECT 1 + 1 test FAILED: Expected result 2, got " + resultValue);
            }

            System.out.println("Spark Thrift JDBC test completed successfully.");

        } catch (SQLException e) {
            System.err.println("SQL Exception occurred during Thrift JDBC test: " + e.getMessage());
            e.printStackTrace();
            System.out.println("Spark Thrift JDBC test FAILED.");
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("Error closing resources: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }
} 