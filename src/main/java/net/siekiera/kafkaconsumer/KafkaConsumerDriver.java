package net.siekiera.kafkaconsumer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by W. Siekiera on 22.01.2018
 */
@Component
public class KafkaConsumerDriver implements CommandLineRunner {
    private int numConsumers = 1;
    private String groupId = "consumer-test-1";
    private List<String> topics = Arrays.asList("tickets");
    private ExecutorService executor = Executors.newFixedThreadPool(numConsumers);
    private List<Future> results = new ArrayList<>();
    private final List<ConsumerLoop> consumers = new ArrayList<>();

    @Override
    public void run(String... strings) throws Exception {
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            results.add(executor.submit(consumer));
        }
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        System.out.println("run() ended.");
    }
}
