package com.mazhara.spark.challenge.configs;

public enum AppCofigs {

    DEL_THRESHOLD("5 minutes"),
    WND_DURATION("15 minutes"),
    UDF_NAME("UniqueUrls"),
    OUTPUT_MODE("complete"),
    OUTPUT_FRMT("memory"),
    APP_NAME("SessionSparkApp"),
    TBL_NAME("SessionsByIP"),
    DEL(","),
    MMR_SIZE("2g");

    public final String label;

    AppCofigs(String label) {
        this.label = label;
    }

}
