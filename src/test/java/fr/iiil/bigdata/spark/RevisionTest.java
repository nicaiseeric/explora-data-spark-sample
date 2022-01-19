package fr.iiil.bigdata.spark;

import fr.iiil.bigdata.spark.beans.Book;
import fr.iiil.bigdata.spark.writer.HBaseWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.*;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class RevisionTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("HBaseWriter-Test").getOrCreate();

    Supplier<Dataset<Book>> bookSupplier = () -> {
        List<Book> books = Arrays.asList(
                Book.builder().title("aladin").price(25f).nbpages(10).build(),
                Book.builder().title("hadoop").price(100f).nbpages(350).build(),
                Book.builder().title("java").price(10f).nbpages(20).build()
        );

        Dataset<Book> bookDataset = sparkSession.createDataset(books, Encoders.bean(Book.class));
        log.info("showing bookDataset...");
        bookDataset.printSchema();
        bookDataset.show(2, false);

        return bookDataset;
    };

    @Test
    public void testSelect() throws IOException {
        Dataset<Book> bookDataset = bookSupplier.get();
        Dataset<Row> ds = bookDataset.select("title", "nbpages");

        log.info("showing select result...");
        ds.printSchema();
        ds.show(false);

        assertThat(ds.columns()).contains("title", "nbpages");

    }

    @Test
    public void testDrop() throws IOException {
        Dataset<Book> bookDataset = bookSupplier.get();
        Dataset<Row> ds = bookDataset.drop("title", "nbpages");

        log.info("showing drop result...");
        ds.printSchema();
        ds.show(false);

        assertThat(ds.columns()).contains("price");

    }

    @Test
    public void testJoin() throws IOException {
        Dataset<Book> bookDataset = bookSupplier.get()
                .union(bookSupplier.get());
        Dataset<Row> stats = bookDataset.groupBy("title")
                .agg(
                        functions.sum("price").as("cost"),
                        functions.avg("nbpages").as("moyp")
                );

        Dataset<Row> ds = bookDataset.distinct()
                .join(stats, "title");


        log.info("showing join result...");
        ds.printSchema();
        ds.show(false);

//        assertThat(ds.columns()).contains("price");

    }

    @Test
    public void testWhere() throws IOException {
        Dataset<Book> bookDataset = bookSupplier.get();
        Column titleLengthCol = functions.length(
                functions.col("title")
        );
        Dataset<Book> ds = bookDataset.where(
                titleLengthCol.geq(
                        functions.lit(5)
                )
        );
//        Dataset<Row> ds2 = bookDataset.where(
//                "length(title) > 3"
//        );

        log.info("showing where result...");
        ds.printSchema();
        ds.show(false);

        assertThat(ds.count()).isEqualTo(2);

    }
}
