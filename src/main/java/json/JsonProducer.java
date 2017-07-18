package json;

/**
 * Created by minal on 18/07/17.
 */

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.codehaus.jackson.JsonNode;

import java.util.Properties;
import java.util.Scanner;

public class JsonProducer {

    private static Scanner in;

    public static void main(String[] argv)throws Exception {
        if(argv.length != 1){
            System.err.println("Please specify 1 Parameter");
            System.exit(-1);
        }

        String topicName = argv[0];
        in = new Scanner(System.in);
        System.out.println("Enter message(type quit to exit)");

        //configure the producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
        Producer producer = new KafkaProducer(configProperties);

        ObjectMapper objectMapper = new ObjectMapper();

        System.out.println("Provide input : ");
        String line = in.nextLine();
        while (!line.equals("exit")) {
            Contact contact = new Contact();
            contact.parseString(line);
            JsonNode jsonNode = objectMapper.valueToTree(contact);
//            JsonNode jsonNode = objectMapper.convertValue(contact, JsonNode.class);
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName, jsonNode);
            producer.send(rec);
            System.out.println("Provide input : ");
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }

}




