package fr.iiil.bigdata.spark.reader;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog;

import java.util.function.Supplier;

@Slf4j
@Builder
@RequiredArgsConstructor
public class HBaseReader implements Supplier<Dataset<Row>> {
    private final SparkSession sparkSession;
    private final String catalog;

    @Override
    public Dataset<Row> get() {
        log.info("reading table from catalog={}", catalog);
        Dataset<Row> ds = sparkSession.emptyDataFrame();
        try {
            ds = sparkSession.read()
                    .option(HBaseTableCatalog.tableCatalog(), catalog)
                    .format("org.apache.spark.sql.execution.datasources.hbase")
                    .load();
        } catch (Exception e){
            log.error("hbase table could not be read due to ...", e);
        }
        return ds;
    }
}
