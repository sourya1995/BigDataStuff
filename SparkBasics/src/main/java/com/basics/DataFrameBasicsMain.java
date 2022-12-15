package com.basics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameBasicsMain{
    private static final String APP_NAME = "DataFrameBasics";
    private static final String LOCAL_NODE_ID = "Local";
    private static final String FORMAT = "csv";


    private static void main(String[] args) {
        DataFrameBasicsMain dataFrameBasicsMain = new DataFrameBasicsMain();
        dataFrameBasicsMain.init();
    }

    private static void init() {
        SparkSession sparkSession = SparkSession.builder()
                .appName(APP_NAME)
                .master(LOCAL_NODE_ID)
                .getOrCreate();

        Dataset<Row> df = sparkSession.read().format(FORMAT)
                .option("header", "true")
                .load("src/main/resources/spark-data/electronic-card-data.csv");

        df.show(15);
    }
}