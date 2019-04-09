package domain;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public  class UserDeserializer implements Deserializer<User> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public User deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        User object = null;
        try {
            object = mapper.readValue(data, User.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }

}
