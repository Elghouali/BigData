package com.bikesharing.analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Bike Sharing Data Analysis using Apache Spark SQL
 *
 * This program loads a CSV dataset containing bike rental transactions
 * and performs multiple analyses using Spark SQL.
 */
public class BikeSharingAnalysis {

    public static void main(String[] args) {

        // ---------------------------------------------------------
        // 1. 2. Create Spark Session
        // ---------------------------------------------------------
        SparkSession spark = SparkSession.builder()
                .appName("Bike Sharing Analysis")
                .master("local[*]")   // Run Spark locally using all CPU cores
                .getOrCreate();


        // ---------------------------------------------------------
        // 1. 2. Load CSV dataset into a Spark DataFrame
        // ---------------------------------------------------------
        Dataset<Row> df = spark.read()
                .option("header", "true")       // First row contains column names
                .option("inferSchema", "true")  // Automatically detect data types
                .csv("data/bike_sharing.csv");  // Path to dataset


        // ---------------------------------------------------------
        // 3. Display Schema
        // ---------------------------------------------------------
        System.out.println("Dataset Schema:");
        df.printSchema();


        // ---------------------------------------------------------
        // 4. Show first 5 rows
        // ---------------------------------------------------------
        System.out.println("First 5 Rows:");
        df.show(5);


        // ---------------------------------------------------------
        // 5. Count total number of rentals
        // ---------------------------------------------------------
        long totalRentals = df.count();
        System.out.println("Total Rentals in Dataset: " + totalRentals);


        // ---------------------------------------------------------
        // 6. Create Temporary SQL View
        // ---------------------------------------------------------
        df.createOrReplaceTempView("bike_rentals_view");


        // =========================================================
        // BASIC SQL QUERIES
        // =========================================================

        // Rentals longer than 30 minutes
        System.out.println("Rentals longer than 30 minutes:");
        spark.sql(
                "SELECT * FROM bike_rentals_view WHERE duration_minutes > 30"
        ).show();


        // Rentals starting at Station A
        System.out.println("Rentals starting at Station A:");
        spark.sql(
                "SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'"
        ).show();


        // Total revenue
        System.out.println("Total Revenue:");
        spark.sql(
                "SELECT SUM(price) AS total_revenue FROM bike_rentals_view"
        ).show();


        // =========================================================
        // AGGREGATION QUERIES
        // =========================================================

        // Rentals per start station
        System.out.println("Rentals per Start Station:");
        spark.sql(
                "SELECT start_station, COUNT(*) AS rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        ).show();


        // Average rental duration per station
        System.out.println("Average Rental Duration per Station:");
        spark.sql(
                "SELECT start_station, AVG(duration_minutes) AS avg_duration " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station"
        ).show();


        // Station with highest rentals
        System.out.println("Station with Highest Rentals:");
        spark.sql(
                "SELECT start_station, COUNT(*) AS rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY start_station " +
                        "ORDER BY rentals DESC " +
                        "LIMIT 1"
        ).show();


        // =========================================================
        // TIME-BASED ANALYSIS
        // =========================================================

        // Extract hour from start_time
        System.out.println("Extract Hour from Start Time:");
        spark.sql(
                "SELECT rental_id, HOUR(start_time) AS rental_hour " +
                        "FROM bike_rentals_view"
        ).show();


        // Rentals per hour (peak hours)
        System.out.println("Rentals per Hour:");
        spark.sql(
                "SELECT HOUR(start_time) AS hour, COUNT(*) AS rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY hour " +
                        "ORDER BY rentals DESC"
        ).show();


        // Most popular station in morning (7–12)
        System.out.println("Most Popular Station in Morning:");
        spark.sql(
                "SELECT start_station, COUNT(*) AS rentals " +
                        "FROM bike_rentals_view " +
                        "WHERE HOUR(start_time) BETWEEN 7 AND 12 " +
                        "GROUP BY start_station " +
                        "ORDER BY rentals DESC " +
                        "LIMIT 1"
        ).show();


        // =========================================================
        // USER BEHAVIOR ANALYSIS
        // =========================================================

        // Average age of users
        System.out.println("Average Age of Users:");
        spark.sql(
                "SELECT AVG(age) AS average_age FROM bike_rentals_view"
        ).show();


        // Users by gender
        System.out.println("Users by Gender:");
        spark.sql(
                "SELECT gender, COUNT(*) AS total_users " +
                        "FROM bike_rentals_view " +
                        "GROUP BY gender"
        ).show();


        // Age group renting the most
        System.out.println("Most Active Age Group:");
        spark.sql(
                "SELECT " +
                        "CASE " +
                        "WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                        "WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                        "WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                        "ELSE '51+' END AS age_group, " +
                        "COUNT(*) AS rentals " +
                        "FROM bike_rentals_view " +
                        "GROUP BY age_group " +
                        "ORDER BY rentals DESC"
        ).show();


        // ---------------------------------------------------------
        // Stop Spark Session
        // ---------------------------------------------------------
        spark.stop();
    }
}