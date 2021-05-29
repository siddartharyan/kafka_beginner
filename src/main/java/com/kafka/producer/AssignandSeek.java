package com.kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AssignandSeek {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerGroupDemo.class);

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

        //assign and seek
        TopicPartition read=new TopicPartition("third_1_topic",1);

        consumer.assign(Arrays.asList(read));

        //seek

        consumer.seek(read,0);

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
