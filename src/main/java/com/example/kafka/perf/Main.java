package com.example.kafka.perf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

@SpringBootApplication
public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);

    static String topic;
    static String groupId = "KafkaPerfConsumer";
    KafkaConsumer<String, String> kafkaConsumer;
    @Value("${KAFKA.SERVERS}")
    String kafkaServers;
    public int messagesReceivedLastSecond = 0;

    public static void main(String[] args) {
        topic = args[0];
        SpringApplication.run(Main.class, args);
    }

    public void initConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.kafkaConsumer = new KafkaConsumer<>(properties);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsuming() {
        initConsumer();
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaConsumer.wakeup();

            // join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            // subscribe consumer to our topic(s)
            kafkaConsumer.subscribe(Arrays.asList(topic));
            logger.info("Kafka consumer subscribed to " + topic);
            // poll for new data
            long stime = System.currentTimeMillis();
            int count = 0;
            while (true) {
                ConsumerRecords<String, String> records =
                        kafkaConsumer.poll(Duration.ofMillis(100));
                long ftime = System.currentTimeMillis();
                count += records.count();
                if( ftime - stime > 1000L ) {
                    this.messagesReceivedLastSecond = count;
                    count = 0;
                    stime = ftime;
                    logger.info("messagesReceivedLastSecond: {}", this.messagesReceivedLastSecond);
                }
            }
        } catch (WakeupException e) {
            // we ignore this as this is an expected exception when closing a consumer
        } finally {
            kafkaConsumer.close(); // this will also commit the offsets if need be
            logger.info("The consumer is now gracefully closed");
        }
    }

}
