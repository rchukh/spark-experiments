package com.github.rchukh.spark.dataset.aggregator;

import com.github.rchukh.spark.SparkBaseTest;
import com.github.rchukh.spark.dataset.aggregator.api.DenseVectorValuesElementsAverageAggregator;
import com.github.rchukh.spark.dataset.aggregator.beans.DoubleArrayAVGHolder;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class AggregatorAPITest extends SparkBaseTest {
    @Test
    @DisplayName("[WIP] Show how custom aggregator API can be used.")
    public void testDenseVectorValuesElementsAverageAggregator() throws Exception {
        // Prepare some custom input data
        List<Row> data = Arrays.asList(
                RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
                RowFactory.create("a", Vectors.dense(2.0, 4.0, 9.0)),
                RowFactory.create("b", Vectors.dense(1.0, 1.0, 1.0)),
                RowFactory.create("b", Vectors.dense(2.0, 3.0, 5.0)),
                RowFactory.create("b", Vectors.dense(3.0, 5.0, 10.0))
        );
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.StringType, false),
                DataTypes.createStructField("vector", new VectorUDT(), false)
        });
        Dataset<Row> dataFrame = SPARK_SESSION.createDataFrame(data, schema);
        // Show input data
        dataFrame.show();
        //+---+--------------+
        //| id|        vector|
        //+---+--------------+
        //|  a| [1.0,2.0,3.0]|
        //|  a| [2.0,4.0,9.0]|
        //|  b| [1.0,1.0,1.0]|
        //|  b| [2.0,3.0,5.0]|
        //|  b|[3.0,5.0,10.0]|
        //+---+--------------+

        // Initialize custom aggregator
        Aggregator<Row, DoubleArrayAVGHolder, Row> aggregator = new DenseVectorValuesElementsAverageAggregator(3);
        Dataset<Row> test = dataFrame.groupBy("id").agg(aggregator.toColumn().as("vectorAvg"));

        // Print schema (mostly to show the issues with inner structure)
        test.printSchema();
        //root
        // |-- id: string (nullable = false)
        // |-- vectorAvg: struct (nullable = true)
        // |    |-- averageValues: array (nullable = false)
        // |    |    |-- element: double (containsNull = false)

        // Show as-is
        test.show(false);
        //+---+-------------------------------------------+
        //|id |vectorAvg                                  |
        //+---+-------------------------------------------+
        //|a  |[WrappedArray(1.5, 3.0, 6.0)]              |
        //|b  |[WrappedArray(2.0, 3.0, 5.333333333333333)]|
        //+---+-------------------------------------------+

        // TODO: Remove the additional wrapping that aggregator currently produces (due to inner Row encoder)
        // Show desired output
        test.select("id", "vectorAvg.averageValues").show(false);
        //+---+-----------------------------+
        //|id |averageValues                |
        //+---+-----------------------------+
        //|a  |[1.5, 3.0, 6.0]              |
        //|b  |[2.0, 3.0, 5.333333333333333]|
        //+---+-----------------------------+
    }

}
