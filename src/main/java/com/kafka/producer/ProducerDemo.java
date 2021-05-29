package com.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        Logger logger= LoggerFactory.getLogger(ProducerDemo.class);
        //create producer properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        //create producer

        KafkaProducer<String,String> producer=new KafkaProducer(props);

        //create a producer record

        for(int i=0;i<10;i++) {

            ProducerRecord<String, String> record = new ProducerRecord("third_1_topic", "hello world "+i);

            //send data
            //async
            producer.send(record);
        }

        //flush and close

        producer.flush();
        producer.close();

    }
}
