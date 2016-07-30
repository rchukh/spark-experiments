package com.github.rchukh.spark.dataset.aggregator.api;

import com.github.rchukh.spark.dataset.aggregator.beans.DoubleArrayAVGHolder;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


public class DenseVectorValuesElementsAverageAggregator extends Aggregator<Row, DoubleArrayAVGHolder, Row> {
    private final int vectorSize;

    public DenseVectorValuesElementsAverageAggregator(int vectorSize) {
        this.vectorSize = vectorSize;
    }

    @Override
    public DoubleArrayAVGHolder zero() {
        // NOTE: This can be easily implemented for unknown-sized rows input, if used with List instead of arrays
        // TODO: Check difference in performance for array-based and list-based implementations.
        return new DoubleArrayAVGHolder(vectorSize);
    }

    @Override
    public DoubleArrayAVGHolder reduce(DoubleArrayAVGHolder buffer, Row input) {
        // TODO: How should null-values be handled?
        if (input != null) {
            double[] sums = buffer.getSums();
            int[] counts = buffer.getCounts();

            DenseVector vector = (DenseVector) input.get(1);
            double[] vectorValues = vector.values();
            for (int i = 0, valuesLength = vectorValues.length; i < valuesLength; i++) {
                sums[i] = sums[i] + vectorValues[i];
                counts[i] = counts[i] + 1;
            }
        }
        return buffer;
    }

    @Override
    public DoubleArrayAVGHolder merge(DoubleArrayAVGHolder buffer1, DoubleArrayAVGHolder buffer2) {
        int length = buffer1.getSums().length;

        double[] buffer1Sums = buffer1.getSums();
        double[] buffer2Sums = buffer2.getSums();
        double[] sums = new double[length];
        for (int i = 0, sumsLength = buffer1Sums.length; i < sumsLength; i++) {
            sums[i] = buffer1Sums[i] + buffer2Sums[i];
        }

        int[] buffer1Counts = buffer1.getCounts();
        int[] buffer2Counts = buffer2.getCounts();
        int[] counts = new int[buffer1.getCounts().length];
        for (int i = 0, sumsLength = buffer1Counts.length; i < sumsLength; i++) {
            counts[i] = buffer1Counts[i] + buffer2Counts[i];
        }
        return new DoubleArrayAVGHolder(sums, counts);
    }

    @Override
    public Row finish(DoubleArrayAVGHolder buffer) {
        double[] sums = buffer.getSums();
        int[] counts = buffer.getCounts();
        double[] result = new double[sums.length];
        for (int i = 0, sumsLength = sums.length; i < sumsLength; i++) {
            result[i] = sums[i] / counts[i];
        }
        return RowFactory.create(new Object[]{result});
    }

    @Override
    public Encoder<DoubleArrayAVGHolder> bufferEncoder() {
        return Encoders.bean(DoubleArrayAVGHolder.class);
    }

    @Override
    public Encoder<Row> outputEncoder() {
        // This should NOT be a RowEncoder, as it creates additional nested structure on top of the schema provided below (which is expected)
        // TODO: Find a way to pass Array directly (with Encoders.kryo it results in binary column).
        // NOTE: In spark scala api (as always), it is as easy as "Encoder[Array[Double]] = ExpressionEncoder()"..
        // ... but in java - https://issues.apache.org/jira/browse/SPARK-13128
        return RowEncoder.apply(DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        "averageValues",
                        DataTypes.createArrayType(
                                DataTypes.DoubleType, false
                        ),
                        false
                )
        }));
    }
}
