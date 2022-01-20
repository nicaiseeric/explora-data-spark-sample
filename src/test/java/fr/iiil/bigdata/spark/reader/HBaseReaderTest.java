package fr.iiil.bigdata.spark.reader;

import fr.iiil.bigdata.spark.beans.Contact;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
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
public class HBaseReaderTest {
    final SparkSession sparkSession = SparkSession.builder().master("local[2]").appName("HBaseReader-Test").getOrCreate();

    private static final String catalogFileName = "src/test/resources/agendacatalog.conf";
    private static String catalog;

    @BeforeClass
    public static void setUp() throws IOException {
        catalog = Files.lines(Paths.get(catalogFileName))
                .map(String::trim)
                .collect(Collectors.joining(""));
    }

    @Test
    public void testHbaseReader() throws IOException {
        List<Contact> expected = Arrays.asList(
                Contact.builder().raison("client|edf").word("energie").address("limoges").build(),
                Contact.builder().raison("societe|bnp").address("paris").build()
        );

        HBaseReader contactReader = new HBaseReader(sparkSession, catalog);

        Dataset<Contact> actual = contactReader.get()
                .as(Encoders.bean(Contact.class));

        log.info("showing actual datafrom hbasereader ...");
        actual.printSchema();
        actual.show(false);

        assertThat(actual.collectAsList()).containsExactlyInAnyOrder(
                expected.toArray(new Contact[0])
        );

    }

}
