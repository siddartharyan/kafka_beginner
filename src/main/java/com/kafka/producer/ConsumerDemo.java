package com.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

        //properties config
        Properties props=new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"my_application");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //create consumer

        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(props);

        //subscribe consumer to topic
        //consumer.subscribe(Collections.singleton())
        consumer.subscribe(Arrays.asList("first_topic","third_topic","third_1_topic"));

        //poll for new data

        while(true){

            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record:records){
                logger.info(
                        record.toString()
                );
            }
        }



    }
}