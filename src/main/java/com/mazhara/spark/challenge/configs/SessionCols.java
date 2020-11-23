package com.mazhara.spark.challenge.configs;

public enum SessionCols {

    TIME("Time"),
    IP("Ip"),
    URL("Url"),
    FIRST_TIME("FirstTime"),
    LAST_TIME("LastTime"),
    SESSION_TIME_SEC("SessionTimeSec"),
    COLLECTED_URLS("CollectedUrls"),
    UNIQUE_URLS_COUNT("UniqueUrlsCount");

    public final String label;

    SessionCols(String label) {
        this.label = label;
    }
}
