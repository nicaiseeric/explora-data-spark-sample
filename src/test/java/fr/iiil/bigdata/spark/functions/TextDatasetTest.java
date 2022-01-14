package fr.iiil.bigdata.spark.functions;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.junit.Test;

@Slf4j
public class TextDatasetTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("DataFrame-Test").getOrCreate();

    @Test
    public void testTXTDataset(){
        Metadata md = new MetadataBuilder().build();
        StructField field1 = new StructField("title", DataTypes.StringType, true, md);
        StructField field2 = new StructField("price", DataTypes.FloatType, true, md);
        StructField field3 = new StructField("nbpages", DataTypes.IntegerType, true, md);
        StructType schema = new StructType(new StructField[]{
                field1, field2, field3
        });
        Dataset<Row> ds = sparkSession.read().text("src/test/resources/books.txt");
        log.info("printing ds");
        ds.printSchema();
        ds.show(false);
        Column filterConditionCol = functions.col("value").startsWith("title,price,nbpages");
        JavaRDD<Row> javaRDD = ds
                .filter(functions.not(filterConditionCol))
                .map(
                        (MapFunction<Row, Row>) row -> {
                            String value = row.getAs("value");
                            String[] fields = StringUtils.splitByWholeSeparatorPreserveAllTokens(value, "," , 3);
                            return RowFactory.create(
                                    fields[0].replaceAll("\"", ""),
                                    NumberUtils.isCreatable(fields[1]) ? Float.parseFloat(fields[1]) : null,
                                    NumberUtils.isCreatable(fields[2]) ? Integer.parseInt(fields[2]) : null
                            );
                        },
                        Encoders.bean(Row.class)
                ).toJavaRDD()
                ;
        Dataset<Row> fds = sparkSession.createDataFrame(javaRDD, schema);
                log.info("printing fds");
        fds.printSchema();
        fds.show(false);

    }


}
