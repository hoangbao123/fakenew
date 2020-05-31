package spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkConnector {
    public static final String APP_NAME = "TEST_SPARK";
    public static final String MASTER = "local[*]";
    private JavaStreamingContext jssc;

    public SparkConnector(){
        SparkConf conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER);
        jssc = new JavaStreamingContext(conf, new Duration(1000));
    }

    public void startTextStream() throws InterruptedException {
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);
        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public void initKafkaReceviver() throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "34.87.20.130:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "my-group");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("fakenew");
        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );

        JavaPairDStream<String, String> records= stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
        records.print();
        jssc.start();
        jssc.awaitTermination();
    }

    public void start() throws InterruptedException {
        jssc.start();
        jssc.awaitTermination();
    }

}
