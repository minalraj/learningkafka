package kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Created by minal on 12/07/17.
 */
public class SimpleProducer {
    // Place holder for producer.
    public static void main(String[] args) {
        Properties kafkaProps = new Properties();

        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProps);


        ProducerRecord<String, String> record = new ProducerRecord<String, String>("CustomerCountry", "Precision Products", "INDIA");
        ProducerRecord<String, String> record1 = new ProducerRecord<String, String>("CustomerCountry", "Biomedical Materials", "CHINA");


        SimpleProducer ob = new SimpleProducer();
        ob.asyncSend(record, kafkaProducer);
        ob.asyncSend(record, kafkaProducer);
        ob.syncSend(record1, kafkaProducer);

    }

    private void asyncSend(ProducerRecord record, KafkaProducer producer) {


        producer.send(record, new DemoProducerCallback());

    }


    private void syncSend(ProducerRecord record, KafkaProducer producer) {
        try {
            producer.send(record).get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }


    private class DemoProducerCallback implements Callback {

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                e.printStackTrace();
            }
        }
    }

}