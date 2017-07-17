package kafka.consumer;

import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by minal on 13/07/17.
 */
public class SimpleConsumer {
    //place holder for consumer


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "CountryCounter");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //Subscribing to topics

        consumer.subscribe(Collections.singletonList("CustomerCountry"));
        Map<String, Integer> custCountryMap = new HashMap<String, Integer>();
//        Map<String, Integer> custCountryMap = new ConcurrentHashMap<String, Integer>();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                   System.out.printf("topic = %s, partition =%s, offset = %d, customer =%s, country =%s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());



                    //synchronous commit
                    try {
                        consumer.commitSync();
                    } catch (CommitFailedException e) {
                        System.out.printf("commit failed");
                    }


                    //commit of specified offset
                    int count=0;
                     Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()+1, "no metadata"));
                    if (count % 1000 == 0)
                        consumer.commitAsync(currentOffsets, null);
                     count++;



                    int updatedCount = 1;
                    if (custCountryMap.containsKey(record.value())) {
                        updatedCount = custCountryMap.get(record.value()) + 1;
                    }
                    custCountryMap.put(record.value(), updatedCount);


                    // Convert custCountryMap to JSON.
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        System.out.println(mapper.writeValueAsString(custCountryMap));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

//                    JSONObject json = new JSONObject(custCountryMap);
//                    System.out.println(json.toString(4));
                }
            }


        }


        finally {
            consumer.close();
        }
    }

}




