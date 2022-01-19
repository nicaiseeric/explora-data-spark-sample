package fr.iiil.bigdata.spark.writer;

import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class HBaseWriter implements Consumer<Dataset<Row>> {
    private final String catalogFileName;

    public HBaseWriter() throws IOException {
        this("src/test/resources/bookcatalog.conf");
    }

    @SneakyThrows
    @Override
    public void accept(Dataset<Row> ds) {
        log.info("catalogFileName={}", catalogFileName);
        String catalog = Files.lines(Paths.get(catalogFileName)).map(String::trim).collect(Collectors.joining(""));

        log.info("catalog={}", catalog);
        ds.write()
                .option(HBaseTableCatalog.tableCatalog(), catalog)
                .option(HBaseTableCatalog.newTable(), "5")
                .format("org.apache.spark.sql.execution.datasources.hbase")
                .save();
    }
}
