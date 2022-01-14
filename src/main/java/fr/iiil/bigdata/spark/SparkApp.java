package fr.iiil.bigdata.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.spark.functions.Count;
import fr.iiil.bigdata.spark.functions.DatasetPrinter;
import fr.iiil.bigdata.spark.functions.WordCountFilterFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

import java.sql.Date;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hello world!
 *
 */
@Slf4j
public class SparkApp {
    public static void main( String[] args ) {
        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String outputPathStr = config.getString("3il.path.output");
        String masterUrl = config.getString("3il.spark.master");

        LocalDate localDate = LocalDate.now();
        Date sqlDate = Date.valueOf(localDate);

        System.out.println( "Hello Spark! Today is " + localDate);



        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("SparkApp");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();


        Dataset<Row> inputDS = sparkSession.read().text(inputPathStr);
        new DatasetPrinter<Row>("inputDS").accept(inputDS);

        Column dateCol = functions.when(
                functions.length(functions.col("value")).$greater(functions.lit(10)),
                functions.lit(sqlDate)
        ).otherwise(
                functions.lit(null)
        );

        Dataset<Row> valueDateDS = inputDS.withColumn("date", dateCol);
        new DatasetPrinter<Row>("valueDateDS").accept(valueDateDS);

        WordCountFilterFunction wordCountFilterFunction = new WordCountFilterFunction(5);
        Dataset<Row> filteredDS = valueDateDS
//                .filter(wordCountFilterFunction)
        .withColumn("length", functions.length(functions.col("value")));
        new DatasetPrinter<Row>("filteredDS").accept(filteredDS);


        List<Count> counts = IntStream.range(0, 10)
                .mapToObj(
                        i -> Count.builder()
                                .date(Date.valueOf(localDate.minusDays(i)))
                                .bool(i%7 == 0)
                                .build()
                ).collect(Collectors.toList());
        log.info("counts = {}", Arrays.deepToString(counts.toArray(new Count[0])));

//        Dataset<Row> countDS1 = sparkSession.createDataFrame(counts, Count.class);
//        Dataset<Count> countDS = countDS1.as(Encoders.bean(Count.class));
        Dataset<Count> countDS = sparkSession.createDataset(counts, Encoders.bean(Count.class));

        log.info(" printing schema of countDS ...");
        countDS.printSchema();
        countDS.show(2, false);
        new DatasetPrinter<Row>("countDS").accept(countDS.toDF());


        Dataset<Row> joinDS = filteredDS
                .join(
                        countDS,
                        filteredDS.col("date").equalTo(countDS.col("date")),
                        "inner"
                )
                .drop(countDS.col("date"));

        new DatasetPrinter<Row>("joinDS").accept(joinDS);


        Dataset<Row> resultDS =  joinDS.select("date", "bool", "length", "value");
        new DatasetPrinter<Row>("resultDS").accept(resultDS);


        resultDS.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("date")
                .parquet(outputPathStr);

    }
}
