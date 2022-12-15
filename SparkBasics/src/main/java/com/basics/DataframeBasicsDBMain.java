package com.basics;

import com.basics.persistence.SalesTransactionsDbManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.basics.persistence.SalesTransactionsDbManager.DB_URL;
import static org.apache.spark.sql.functions.lit;

public class DataframeBasicsDBMain {
    private static final Logger LOGGER = (Logger) LoggerFactory.getLogger(DataframeBasicsDBMain.class);
    private static final String SPARK_FILES_FORMAT = "csv";
    public static final String PATH_RESOURCES = "src/main/resources/spark-data/electronic-card-transactions.csv";
    public static final String PATH_FOLDER_RESOURCES = "src/main/resources/spark-data/enriched_transactions";

    public static void main(String[] args) {
        DataframeBasicsDBMain dataframeBasicsDBMain = new DataframeBasicsDBMain();
        dataframeBasicsDBMain.init();

    }

    private void init() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, IOException {
        deletePreviousFiles(PATH_FOLDER_RESOURCES);
        SalesTransactionsDbManager salesTransactionsDbManager = new SalesTransactionsDbManager();
        salesTransactionsDbManager.startDB();

        SparkSession sparkSession = SparkSession.builder()
                .appName("DataFrameBasicsDB")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = sparkSession.read()
                .format(SPARK_FILES_FORMAT)
                .option("header", "true")
                .load(PATH_RESOURCES);

        Dataset<Row> results = df.withColumn("total_units_value", lit(df.col("Data_value").multiply(df.col("magnitude"))));

        Properties prop = salesTransactionsDbManager.buildDBProperties();
        results.write().mode(SaveMode.Overwrite)
                .jdbc(DB_URL, SalesTransactionsDbManager.TABLE_NAME, prop);

        salesTransactionsDbManager.displayTableResults(5);
        results.write().format(SPARK_FILES_FORMAT)
                .mode(SaveMode.Overwrite)
                .save("src/main/resources/spark-data/enriched_transactions");

        results.show(5);

    }

    private void deletePreviousFiles(String resourcesPath) throws IOException {
        Stream<Path> allContents = Files.list(Paths.get(resourcesPath));
        allContents.sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);

    }
}
