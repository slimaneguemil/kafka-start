import domain.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class ProducerAppUser {

    public static void main(String[] args){

        // Create the Properties class to instantiate the Consumer with the desired settings:
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("bootstrap.servers", "34.234.35.134:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "domain.UserSerializer");


        KafkaProducer<String, User> myProducer = new KafkaProducer<String, User>(props);
        DateFormat dtFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");
        String topic = "my_topic";

        int numberOfRecords = 2; // number of records to send
        long sleepTimer = 1000; // how long you want to wait before the next record to be sent

        try {
                for (int i = 0; i < numberOfRecords; i++ ){
                    User user = new User("dede",40);
                    myProducer.send(new ProducerRecord<String, User>(topic, user));
                    System.out.println(
                            "pushing : " +
                            String.format(
                                    "Message: %s  sent at %s",
                                    Integer.toString(i),
                                    dtFormat.format(new Date())
                                    )
                            );
                    Thread.sleep(sleepTimer);
                    // Thread.sleep(new Random(5000).nextLong()); // use if you want to randomize the time between record sends
                }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myProducer.close();
        }

    }
}
