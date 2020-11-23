package com.mazhara.spark.challenge.configs;

public enum SparkOptions {

    DEL("delimiter"),
    MASTER("local[*]"),
    DR_MMR("spark.driver.memory"),
    EX_MMR("spark.executor.memory"),
    LOG_LEVEL("ERROR");

    public final String label;

    SparkOptions(String label) {
        this.label = label;
    }

}
