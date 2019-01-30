package com.git.shelly.kafka.tutorial;

import org.apache.kafka.clients.producer.*;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException ;

public class ProducerDemoKeys {

    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {

     final   Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class) ;
        System.out.println(" hello world!") ;
        final String BOOT_STRAP_SERVER="127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOT_STRAP_SERVER) ;
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(properties);

        for (int i =0; i <10; i++) {

            String topic ="new_topic2" ;
            String value ="hello world " + Integer.toString(i) ;
            String key = "id_" + Integer.toString(i) ;
            logger.info("key: "+ key) ;

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("new_topic2",key, "hello world");
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //record was successfully sent
                        logger.info("Received new metadata . \n" +

                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else {
                        e.printStackTrace();
                        logger.error("error" + e);
                    }
                }
            }).get(1,TimeUnit.SECONDS);
        }
        producer.flush();
        producer.close();
    }
}
