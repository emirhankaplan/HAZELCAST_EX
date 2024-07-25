package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

public class RandomNumbersPerformance {

    // Change these constants to your Azure SQL Database details
    private static final String JDBC_URL = "jdbc:sqlserver://localhost:1433;database=master;encrypt=true;trustServerCertificate=true";
    private static final String JDBC_USER = "SA";
    private static final String JDBC_PASSWORD = "reallyStrongPwd123";

    public static void main(String[] args) {
        try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD)) {
            // Create table
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("IF OBJECT_ID('dbo.RandomNumbers', 'U') IS NOT NULL DROP TABLE dbo.RandomNumbers;");
                stmt.execute("CREATE TABLE dbo.RandomNumbers (ID INT IDENTITY(1,1) PRIMARY KEY, Number INT);");
            }

            // Insert 20,000 random numbers
            long insert20000Time = insertRandomNumbers(conn, 20000);
            System.out.println("Time taken to insert 20,000 random numbers: " + insert20000Time + " milliseconds");

            // Select 20 random numbers from 20,000
            long select20000Time = selectRandomNumbers(conn, 20);
            System.out.println("Time taken to select 20 random numbers from 20,000: " + select20000Time + " milliseconds");

            // Insert 100,000 random numbers
            long insert100000Time = insertRandomNumbers(conn, 100000);
            System.out.println("Time taken to insert 100,000 random numbers: " + insert100000Time + " milliseconds");

            // Select 100 random numbers from 100,000
            long select100000Time = selectRandomNumbers(conn, 100);
            System.out.println("Time taken to select 100 random numbers from 100,000: " + select100000Time + " milliseconds");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long insertRandomNumbers(Connection conn, int count) throws Exception {
        long startTime = System.currentTimeMillis();

        String insertSql = "INSERT INTO dbo.RandomNumbers (Number) VALUES (?)";
        try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
            Random rand = new Random();
            for (int i = 0; i < count; i++) {
                pstmt.setInt(1, rand.nextInt(1000000)); // Adjust the range if needed
                pstmt.addBatch();
                if (i % 1000 == 0) {
                    pstmt.executeBatch(); // Execute every 1000 inserts
                }
            }
            pstmt.executeBatch(); // Execute remaining batch
        }

        return System.currentTimeMillis() - startTime;
    }

    private static long selectRandomNumbers(Connection conn, int numberOfRecords) throws Exception {
        long startTime = System.currentTimeMillis();

        String selectSql = "SELECT TOP (?) Number FROM dbo.RandomNumbers ORDER BY NEWID()";
        try (PreparedStatement pstmt = conn.prepareStatement(selectSql)) {
            pstmt.setInt(1, numberOfRecords);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    // Fetch each number to avoid optimization
                    rs.getInt("Number");
                }
            }
        }

        return System.currentTimeMillis() - startTime;
    }
}
