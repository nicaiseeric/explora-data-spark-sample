package fr.iiil.bigdata.spark.functions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;

import java.util.function.Consumer;

@Slf4j
@RequiredArgsConstructor
public class DatasetPrinter<T> implements Consumer<Dataset<T>> {
    private final String datasetName;
    @Override
    public void accept(Dataset<T> dataset) {
        Dataset<T> cachedDataset = dataset.cache();

        log.info("********************\n");
        log.info("Printing {}", datasetName);

        log.info("Printing {}.count()={}", datasetName, cachedDataset.count());
        log.info("Printing {}.distinct().count()={}", datasetName, cachedDataset.distinct().count());

        log.info("Printing {}.schema", datasetName);
        cachedDataset.printSchema();

        log.info("Printing {}.show()", datasetName);
        cachedDataset.show(5, false);

        cachedDataset.unpersist();

    }
}
