import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class App2 {
    public static void main(String[] args) {
        // 1. Configuration Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-processing-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        // 2. Construire le flux Kafka
        StreamsBuilder builder = new StreamsBuilder();

        // Source Processor: Lire les commandes clients depuis le topic "orders"
        KStream<String, String> ordersStream = builder.stream("orders");

        // Stream Processor 1: Filtrer les commandes supérieures à 100
        KStream<String, String> filteredOrders = ordersStream.filter((key, value) -> {
            String[] parts = value.split(",");
            double amount = Double.parseDouble(parts[1]); // Example format: "Alice,200.0"
            return amount > 100.0;
        });

        // Stream Processor 2: Ajouter une taxe de 10% au montant des commandes
        KStream<String, String> ordersWithTax = filteredOrders.mapValues(value -> {
            String[] parts = value.split(",");
            String client = parts[0];
            double amount = Double.parseDouble(parts[1]);
            double amountWithTax = amount * 1.1; // Ajouter 10% de taxe
            return client + "," + amountWithTax;
        });

        // Stream Processor 3: Grouper par client
        KGroupedStream<String, String> groupedOrders = ordersWithTax.groupBy((key, value) -> value.split(",")[0]);

        // Stream Processor 4: Calculer la somme totale des commandes (KTable)
        KTable<String, Double> totalByCustomer = groupedOrders.aggregate(
                () -> 0.0, // Valeur initiale
                (key, value, aggregate) -> {
                    double amount = Double.parseDouble(value.split(",")[1]);
                    return aggregate + amount;
                },
                Materialized.with(Serdes.String(), Serdes.Double())
        );

        // Sink Processor: Écrire les résultats dans le topic "customer-total"
        // Convert KTable back to KStream to output to a Kafka topic
        totalByCustomer.toStream().to("customer-total");

        // 3. Démarrer Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 4. Arrêt propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}