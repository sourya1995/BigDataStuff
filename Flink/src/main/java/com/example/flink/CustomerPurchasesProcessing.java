package com.example.flink;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CustomerPurchasesProcessing {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStream<Tuple4<String, String, Integer, String>> inputStream = env.fromElements(
                Tuple4.of("William", "TV", 1500, "Amazon"),
                Tuple4.of("William", "iPad", 499, "Walmart"),
                Tuple4.of("John", "Fitbit", 359, "Amazon"),
                Tuple4.of("Tom", "Samsung Galaxy", 556, "Target"),
                Tuple4.of("Tom", "Headphones", 89, "Amazon"),
                Tuple4.of("Alvin", "Smart Wi-Fi Camera", 99, "Walmart"),
                Tuple4.of("Kevin", "Airpods", 199, "Apple Store"),
                Tuple4.of("Kevin", "Headphones", 53, "Walmart"),
                Tuple4.of("Henry", "Watch", 128, "Target")
        );

        tableEnv.createTemporaryView("CustomerPurchases", inputStream, $("Name"), $("Product"), $("Price"), $("Store"));
        TableResult tableResult = tableEnv.sqlQuery("SELECT * FROM CustomerPurchases").execute();
        tableResult.print();
    }
}
