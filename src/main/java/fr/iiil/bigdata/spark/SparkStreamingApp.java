package fr.iiil.bigdata.spark;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import fr.iiil.bigdata.spark.functions.Count;
import fr.iiil.bigdata.spark.functions.DatasetPrinter;
import fr.iiil.bigdata.spark.functions.WordCountFilterFunction;
import fr.iiil.bigdata.spark.receiver.KafkaReceiver;
import fr.iiil.bigdata.spark.writer.HBaseWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

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
public class SparkStreamingApp {
    public static void main( String[] args ) {
        Config config = ConfigFactory.load();
        String inputPathStr = config.getString("3il.path.input");
        String checkpointPathStr = config.getString("3il.path.checkpoint");
        List<String> topics= config.getStringList("3il.topics");
        String outputPathStr = config.getString("3il.path.output");
        String masterUrl = config.getString("3il.spark.master");

        LocalDate localDate = LocalDate.now();
        Date sqlDate = Date.valueOf(localDate);

        System.out.println( "Hello Spark! Today is " + localDate);



        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("SparkApp");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(
                checkpointPathStr,
                () -> new JavaStreamingContext(sparkSession.sparkContext().getConf(),new Duration(10000)),
                sparkSession.sparkContext().hadoopConfiguration()
        );

        KafkaReceiver kafkaReceiver = new KafkaReceiver(javaStreamingContext,topics);
        JavaDStream<String> inputDStream = kafkaReceiver.get();
        inputDStream.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                        Dataset<Row> inputDS = sparkSession.createDataFrame(stringJavaRDD,Row.class);
                        new DatasetPrinter<Row>("inputDS"+time.toString()).accept(inputDS);

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


//                        resultDS
//                                .write()
//                                .mode(SaveMode.Overwrite)
//                                .partitionBy("date")
//                                .parquet(outputPathStr + "/time=" + time.milliseconds());
//
                        Dataset<Row> outputDS = resultDS
                                .withColumn("time", functions.lit(time.milliseconds()))
                                .cache();

                        outputDS
                                .write()
                                .mode(SaveMode.Overwrite)
                                .partitionBy("date", "time")
                                .parquet(outputPathStr);

                        new HBaseWriter().accept(outputDS);

                        outputDS.unpersist();

                    }
                }
        );


    }
}
