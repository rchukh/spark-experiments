package com.github.rchukh.spark.dataset.aggregator.beans;

import java.util.Arrays;

public class DoubleArrayAVGHolder {
    private double[] sums;
    private int[] counts;

    public DoubleArrayAVGHolder() {
    }

    public DoubleArrayAVGHolder(int size) {
        this.sums = new double[size];
        this.counts = new int[size];
    }

    public DoubleArrayAVGHolder(double[] sums, int[] counts) {
        this.sums = sums;
        this.counts = counts;
    }

    public double[] getSums() {
        return sums;
    }

    public void setSums(double[] sums) {
        this.sums = sums;
    }

    public int[] getCounts() {
        return counts;
    }

    public void setCounts(int[] counts) {
        this.counts = counts;
    }

    @Override
    public String toString() {
        return "DoubleArrayAVGHolder{" +
                "sums=" + Arrays.toString(sums) +
                ", counts=" + Arrays.toString(counts) +
                '}';
    }
}
