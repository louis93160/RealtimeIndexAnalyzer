import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        // Configuration des propriétés du consommateur Kafka
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "stock-data-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Création du consommateur Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Abonnement au sujet "stock-data"
        consumer.subscribe(Collections.singletonList("stock-data"));

        // Objet pour désérialiser les messages JSON
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            while (true) {
                // Lecture des messages
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // Désérialisation du message JSON
                    String jsonData = record.value();
                    Object data = objectMapper.readValue(jsonData, Object.class);
                    // Affichage des données
                    System.out.println(data);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
