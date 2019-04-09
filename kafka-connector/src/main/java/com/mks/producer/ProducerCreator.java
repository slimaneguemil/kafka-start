package com.mks.producer;


import java.util.Properties;

import com.mks.constants.IKafkaConstants;
import com.mks.pojo.User;
import com.mks.serializer.UserSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerCreator {

    public static Producer<String, User> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IKafkaConstants.KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, UserSerializer.class.getName());


//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        //props.put("bootstrap.servers", "34.234.35.134:9092");
//        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.put("value.serializer", "domain.UserSerializer");
        return new KafkaProducer<String, User>(props);
    }

}
