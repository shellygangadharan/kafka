package com.git.shelly.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemo {
   private  final static  Logger logger  = LoggerFactory.getLogger(ConsumerDemo.class.getName()) ;



    public static void main(String[] args) throws InterruptedException {


        System.out.println("hello world!");
        String bootStrapServers ="127.0.0.1:9092";
        String groupId = "my-sixth-application" ;
        String topic = "new_topic2" ;
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerThread  thread = new ConsumerThread(bootStrapServers,groupId,topic,latch);
        Runtime.getRuntime().addShutdownHook( new Thread(  ()-> {
            logger.info(" caught shutdown hook") ;
            thread.shutdown();
            try {
                latch.await();
            }catch(InterruptedException e){
                e.printStackTrace();
            }
        }));
        new Thread(thread).start() ;

        latch.await();
    }

    static class ConsumerThread implements Runnable {

        private CountDownLatch latch ;

        private KafkaConsumer<String,String> consumer ;

        public ConsumerThread(String bootstrapServers,String groupId, String topic, CountDownLatch latch){
            this.latch = latch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId) ;
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest") ;
            consumer = new KafkaConsumer<String, String>(properties) ;
            consumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run () {

            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("Key: " + record.key() + ", value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                }
            }catch(WakeupException e){
                logger.info("Received shutdown");
            }
            finally{
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }

    }
}
