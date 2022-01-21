package fr.iiil.bigdata.spark.writer;

import fr.iiil.bigdata.spark.beans.Book;
import fr.iiil.bigdata.spark.reader.HBaseReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
public class HBaseWriterTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("HBaseWriter-Test").getOrCreate();

    private static final String catalogFileName = "src/test/resources/bookcatalog.conf";
    private static String catalog;

    @BeforeClass
    public static void setUp() throws IOException {
        catalog = Files.lines(Paths.get(catalogFileName)).map(String::trim).collect(Collectors.joining(""));
    }

    @Test
    public void testHbaseWriter() throws IOException {
        List<Book> books = Arrays.asList(
                Book.builder().title("aladin").price(25f).nbpages(10).build(),
                Book.builder().title("hadoop").price(100f).nbpages(350).build(),
                Book.builder().title("java").price(10f).nbpages(20).build()
        );

        Dataset<Book> bookDataset = sparkSession.createDataset(books, Encoders.bean(Book.class));
        bookDataset.printSchema();
        bookDataset.show(2, false);

        HBaseWriter bookWriter = new HBaseWriter();
        bookWriter.accept(bookDataset.toDF());

        Dataset<Book> actual = sparkSession.read()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .load()
                .as(Encoders.bean(Book.class));

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(
                bookDataset.collectAsList().toArray(new Book[0])
        );

    }

}
