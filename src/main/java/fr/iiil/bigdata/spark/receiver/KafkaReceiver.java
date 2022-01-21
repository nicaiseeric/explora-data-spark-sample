package fr.iiil.bigdata.spark.receiver;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@AllArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<String>>  {
    private final JavaStreamingContext javaStreamingContext;
    private List<String> topics;
    private final Map<String,Object> kafkaparam = new HashMap<String,Object>()
    {{
       put("bootstrap.servers","localhost:9092");
       put("key.deserializer", StringDeserializer.class);
       put("value.deserializer",StringDeserializer.class);
       put("group.id","spark-kafta-integ");
        put("auto.offset.reset","earliest");

    }};
    @Override
    public JavaDStream<String> get() {
        JavaInputDStream<ConsumerRecord<String,String>> directStream = KafkaUtils.createDirectStream(
                javaStreamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics,kafkaparam)
        );
        JavaDStream<String> javaDStream = directStream.map(ConsumerRecord::value);

        return javaDStream;
    }
}
