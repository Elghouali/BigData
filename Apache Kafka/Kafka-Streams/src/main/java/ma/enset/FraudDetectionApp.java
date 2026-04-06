import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class FraudDetectionApp {
    public static void main(String[] args) {
        // 1. Configuration Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fraud-detection-text-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 2. Lecture des transactions sous forme de texte depuis "transactions-input"
        KStream<String, String> transactions = builder.stream("transactions-input");

        // 3. Transformation: Détection de fraude (Montant > 10 000)
        KStream<String, String> suspiciousTransactions = transactions.filter((key, value) -> {
            // Séparer les champs par virgule
            String[] parts = value.split(",");

            // Validation du format: il faut au moins 3 champs (ex: id,montant,compte)
            if (parts.length < 3) return false;

            try {
                double amount = Double.parseDouble(parts[1]);
                return amount > 10000; // Filtrer les montants > 10 000
            } catch (NumberFormatException e) {
                return false; // Ignorer les messages mal formatés
            }
        });

        // 4. Écrire les transactions suspectes dans le topic "fraud-alerts"
        suspiciousTransactions.to("fraud-alerts");

        // 5. Construire et démarrer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // 6. Arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}