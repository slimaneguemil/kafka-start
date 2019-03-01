import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;

public class ConsumerGrp01_A {
    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("bootstrap.servers", "34.234.35.134:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        // Create a KafkaConsumer instance and configure it with properties.
        KafkaConsumer<String, String> myConsumer = new KafkaConsumer<String, String>(props);

        // Create a topic subscription list:
        ArrayList<String> topics = new ArrayList<String>();
        topics.add("my_topic");
        myConsumer.subscribe(topics);


        // Start polling for messages:
        int messageProcessed =0;
        System.out.println("Listening message from topic=my_topic ...");
        try {
            while (true){
                ConsumerRecords<String,String> records = myConsumer.poll(10);
                //printRecords(records);
                for (ConsumerRecord<String,String> record: records){

                      //  sleep(6000);

                    messageProcessed++;
                    System.out.println(String.format(
                            "Topic %s, Partition: %d, Offset: %d, Key: %s, Value: %s ",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value()
                            )
                    );
                }
                //System.out.println(String.format("total message processed %d",messageProcessed));

            }
        } finally {
            myConsumer.close();
        }

    }

    private static void printSet(Set<TopicPartition> collection){
        if (collection.isEmpty()) {
            System.out.println("I do not have any partitions assigned yet...");
        }
        else {
            System.out.println("I am assigned to following partitions:");
            for (TopicPartition partition: collection){
                System.out.println(String.format("Partition: %s in Topic: %s", Integer.toString(partition.partition()), partition.topic()));
            }
        }
    }

    private static void printRecords(ConsumerRecords<String, String> records)
    {
        for (ConsumerRecord<String, String> record : records) {
            System.out.println(String.format("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s", record.topic(), record.partition(), record.offset(), record.key(), record.value()));

        }


    }
}
