package fr.iiil.bigdata.spark.functions;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DataFrameTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("DataFrame-Test").getOrCreate();

    @Test
    public void testDataFrameFromList(){
        List<Row> rows = Arrays.asList(
                RowFactory.create("aladin", 25f, 10),
                RowFactory.create("hadoop", 100f, 350),
                RowFactory.create("hadoop", 10f, 350)
        );
        Metadata md = new MetadataBuilder().build();
        StructField field1 = new StructField("title", DataTypes.StringType, true, md);
        StructField field2 = new StructField("price", DataTypes.FloatType, true, md);
        StructField field3 = new StructField("nbpages", DataTypes.IntegerType, true, md);
        StructType schema = new StructType(new StructField[]{
                field1, field2, field3
        });
        Dataset<Row> ds = sparkSession.createDataFrame(rows, schema);
        ds.printSchema();
        ds.show(false);
    }


    @Test
    public void testDataFrameFromRDD(){
        List<Row> rows = Arrays.asList(
                RowFactory.create("aladin", 25f, 10),
                RowFactory.create("hadoop", 100f, 350),
                RowFactory.create("hadoop", 10f, 350)
        );
        JavaRDD<Row> javaRDD = JavaSparkContext.fromSparkContext(sparkSession.sparkContext()).parallelize(rows, 2);
        Metadata md = new MetadataBuilder().build();
        StructField field1 = new StructField("title", DataTypes.StringType, true, md);
        StructField field2 = new StructField("price", DataTypes.FloatType, true, md);
        StructField field3 = new StructField("nbpages", DataTypes.IntegerType, true, md);
        StructType schema = new StructType(new StructField[]{
                field1, field2, field3
        });
        Dataset<Row> ds = sparkSession.createDataFrame(javaRDD, schema);
        ds.printSchema();
        ds.show(false);
    }

}
