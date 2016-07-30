package com.github.rchukh.spark;


import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public abstract class SparkBaseTest {
    protected static SparkSession SPARK_SESSION;

    @BeforeAll
    public static void prepare() {
        SparkSession.Builder configBuilder = SparkSession
                .builder()
                .master("local")
                .appName("Spark Experiments")
//                .config("spark.cores.max", coresMax)
//                .config("spark.driver.memory", driverMemory)
//                .config("spark.executor.memory", executorMemory)
//                .config("spark.serializer", serializer)
//                .config("spark.kryoserializer.buffer.max", kryoserializerBufferMax)
//                .config("spark.kryo.registrationRequired", "false")
                .config("spark.default.parallelism", 4)
                .config("spark.sql.shuffle.partitions", 1);
        // Windows-related issue in 2.0:
        // - https://issues.apache.org/jira/browse/SPARK-15893
        // - https://issues.apache.org/jira/browse/SPARK-15899
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            // Workaround: Use exactly the same default value, but set "file:/" instead of "file:"
            configBuilder = configBuilder.config("spark.sql.warehouse.dir", "file:/" + System.getProperty("user.dir") + "/spark-warehouse");
        }
        SPARK_SESSION = configBuilder.getOrCreate();
    }

    @AfterAll
    public static void tearDown() {
        SPARK_SESSION.stop();
    }

}
