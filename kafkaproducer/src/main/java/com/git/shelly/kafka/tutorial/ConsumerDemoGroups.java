package com.git.shelly.kafka.tutorial;

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

public class ConsumerDemoGroups {

    public static void main(String[] args){

        Logger logger  = LoggerFactory.getLogger(ConsumerDemoGroups.class.getName()) ;
        String bootStrapServers ="127.0.0.1:9092";
        String groupId = "my-fifth-application" ;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId) ;
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest") ;
        System.out.println("hello world!");

        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String> (properties) ;
        consumer.subscribe(Arrays.asList("new_topic2"));

        while(true) {
          ConsumerRecords<String, String> records=  consumer.poll(Duration.ofMillis(100))  ;
            for(ConsumerRecord<String, String> record :records){
                logger.info("Key:" +record.key()+ ", value :"+ record.value() );
                logger.info("Partition:" +record.partition()+ ", value :"+ record.offset() );
            }
        }
    }
}
