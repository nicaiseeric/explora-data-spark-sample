package fr.iiil.bigdata.spark.functions;

import lombok.RequiredArgsConstructor;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

@RequiredArgsConstructor
public class WordCountFilterFunction implements FilterFunction<Row> {
    private final Integer threshold;

    @Override
    public boolean call(Row row) throws Exception {
        String value = row.getAs("value");
        if(value == null){
            return false;
        }

        try{
            String[] words = value.split("\\W");
            return words.length > threshold;
        } catch (Exception ex){
            return false;
        }
    }
}
