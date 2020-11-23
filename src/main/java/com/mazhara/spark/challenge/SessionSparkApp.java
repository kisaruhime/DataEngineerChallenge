package com.mazhara.spark.challenge;

import com.mazhara.spark.challenge.configs.AppCofigs;
import com.mazhara.spark.challenge.configs.SessionCols;
import com.mazhara.spark.challenge.configs.SparkOptions;
import lombok.val;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.mutable.WrappedArray;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.types.DataTypes.*;

public class SessionSparkApp {


    public static void main(String[] args) throws Exception {

        val CSV_DIRECTORY = args[0];

        val sparkSession = init();
        val sessionSchema = defineSchema();

        val sessionDF = sparkSession
                .readStream()
                .schema(sessionSchema)
                .option(SparkOptions.DEL.label, AppCofigs.DEL.label)
                .csv(CSV_DIRECTORY);

       val ipAggDF = sessionDF
                .withWatermark(SessionCols.TIME.label, AppCofigs.DEL_THRESHOLD.label)
                .groupBy(
                        window(col(SessionCols.TIME.label), AppCofigs.WND_DURATION.label, AppCofigs.WND_DURATION.label),
                        col(SessionCols.IP.label))
                .agg(functions.collect_list(SessionCols.URL.label).as(SessionCols.COLLECTED_URLS.label),
                        functions.first(SessionCols.TIME.label).as(SessionCols.FIRST_TIME.label),
                        functions.last(SessionCols.TIME.label).as(SessionCols.LAST_TIME.label))
                .withColumn(SessionCols.SESSION_TIME_SEC.label,
                        col(SessionCols.LAST_TIME.label).cast(LongType).minus(col(SessionCols.FIRST_TIME.label).cast(LongType)).cast(LongType));


        val query = ipAggDF
                .withColumn(SessionCols.UNIQUE_URLS_COUNT.label,
                        functions.callUDF(AppCofigs.UDF_NAME.label, col(SessionCols.COLLECTED_URLS.label)))
                .writeStream()
                .queryName(AppCofigs.TBL_NAME.label)
                .outputMode(AppCofigs.OUTPUT_MODE.label)
                .format(AppCofigs.OUTPUT_FRMT.label)
                .start();

        query.awaitTermination(200000);

//      Average session time
        sparkSession.sql("select avg(SessionTimeSec) AverageSessionTime from SessionsByIP").show();

//      Average unique URL visits per session
        sparkSession.sql("select avg(UniqueUrlsCount) as AverageUniqueUrlsPerSession from SessionsByIP").show();

//      Ips have the bigger amount of unique URL visits per session
        sparkSession.sql("select IP, max(UniqueUrlsCount) as MaxUniqueUrlsPerSession from SessionsByIP " +
                "group by IP order by MaxUniqueUrlsPerSession desc").show();

//      Most engaged users
        sparkSession.sql("select IP, max(SessionTimeSec) as MaxSessionDuration from SessionsByIP " +
                "group by IP order by MaxSessionDuration desc").show();


        sparkSession.stop();

    }

    public static SparkSession init() {
        SparkSession sparkSession = SparkSession
                .builder()
                .master(SparkOptions.MASTER.label)
                .config(SparkOptions.DR_MMR.label, AppCofigs.MMR_SIZE.label)
                .config(SparkOptions.EX_MMR.label, AppCofigs.MMR_SIZE.label)
                .appName(AppCofigs.APP_NAME.label)
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel(SparkOptions.LOG_LEVEL.label);

//        Aim of this udf to retrieve count of unique urls per ip session by at first converting list of urls
//        to map with occurrences as value, and then by filtering and counting
        sparkSession.sqlContext().udf().register(AppCofigs.UDF_NAME.label, new UDF1<WrappedArray<String>, Long>() {
            @Override
            public Long call(WrappedArray<String> arrayListWrappedArray){
                List<String> listOfUrls = new ArrayList(JavaConverters
                        .asJavaCollectionConverter(arrayListWrappedArray)
                        .asJavaCollection());
                return listOfUrls.stream()
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                        .entrySet()
                        .stream()
                        .filter(a -> a.getValue().equals(1L))
                        .count();
            }
        }, DataTypes.LongType);

        return sparkSession;
    }

    public static StructType defineSchema() {
        return new StructType()
                .add(SessionCols.TIME.label, TimestampType)
                .add(SessionCols.IP.label, StringType)
                .add(SessionCols.URL.label, StringType);
    }
}
