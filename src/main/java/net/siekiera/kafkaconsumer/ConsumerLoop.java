package net.siekiera.kafkaconsumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

/**
 * Created by W. Siekiera on 22.01.2018
 */
public class ConsumerLoop implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public ConsumerLoop(int id,
                        String groupId,
                        List<String> topics) {
        this.id = id;
        this.topics = topics;
        Properties props = new Properties();
        props.put("bootstrap.servers", "77.55.214.96:9092");
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("max.poll.interval.ms", 1000);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        this.consumer = new KafkaConsumer<>(props);
    }

    @Override
    public void run() {


        try {
            consumer.subscribe(topics);

            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                long pollTime = System.nanoTime();
                System.out.println("Pobrałem " + records.count() + " rekordów.");
                System.out.println("Zaczynam procesowanie ...");
                for (ConsumerRecord<String, String> record : records) {
                    long singleRecordProcessingStartTime = System.nanoTime();
                    Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataHashMap = new HashMap<>();
                    topicPartitionOffsetAndMetadataHashMap.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    try {
                        consumer.commitSync(topicPartitionOffsetAndMetadataHashMap);
                    } catch (Exception e) {
                        Thread.sleep(10000);
                        //System.out.println(e.getMessage());
                    }
                    long finishTime = System.nanoTime();
                    long duration = finishTime - singleRecordProcessingStartTime;
                    long timeSinceLastPoll = finishTime - pollTime;
                    System.out.println("Offset=" + record.offset() + " single processing=" + (duration / 1000000) + " time since last poll=" + (timeSinceLastPoll / 1000000));

                }
            }
        } catch (Exception e) {
            System.out.println("Exception occured: " + e.getMessage());
        } finally {
            consumer.close();
        }
        System.out.println("It's all fucked up.");
    }

    public void shutdown() {
        consumer.wakeup();
    }
}
