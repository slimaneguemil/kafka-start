package com.mks;


import java.util.concurrent.ExecutionException;

import com.mks.constants.IKafkaConstants;
import com.mks.consumer.ConsumerCreator;
import com.mks.pojo.User;
import com.mks.producer.ProducerCreator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


public class App {

    public static void main(String[] args) {
		runProducer();
        runConsumer();
    }
    static void runConsumer() {
        Consumer<String, User> consumer = ConsumerCreator.createConsumer();

        int noMessageToFetch = 0;

        while (true) {
            final ConsumerRecords<String, User> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }
            for (ConsumerRecord<String,User> record: consumerRecords){

//                    System.out.println(String.format(
//                            "Topic %s, Partition: %d, Offset: %d, Key: %s, Value: %s ",
//                            record.topic(), record.partition(), record.offset(), record.key(), record.value()
//                            )
//                    );

                System.out.println("Message received " + record.value().toString());
            }
//            consumerRecords.forEach(record -> {
//                System.out.println("Record Key " + record.key());
//                System.out.println("Record value " + record.value());
//                System.out.println("Record partition " + record.partition());
//                System.out.println("Record offset " + record.offset());
//            });
            consumer.commitAsync();
        }
        consumer.close();
    }

    static void runProducer() {
        Producer<String, User> producer = ProducerCreator.createProducer();

        for (int index = 1; index < IKafkaConstants.MESSAGE_COUNT; index++) {
            User user = new User("toto",index);
            final ProducerRecord<String, User> record = new ProducerRecord<String, User>(IKafkaConstants.TOPIC_NAME, user);
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Record sent with key " + index + " to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } catch (ExecutionException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            } catch (InterruptedException e) {
                System.out.println("Error in sending record");
                System.out.println(e);
            }
        }
    }

}
