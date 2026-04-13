import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class GlobalClickCounterApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-click-counter-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable cache for instant updates

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Lire les événements de clics
        KStream<String, String> clicksStream = builder.stream("clicks");

        // 2. Grouper TOUS les clics sous une même clé ("global") pour avoir un total absolu
        KTable<String, Long> globalClickCounts = clicksStream
                .groupBy((key, value) -> "global")
                .count(Materialized.as("global-counts-store"));

        // 3. Convertir le compte en String et l'envoyer au topic de sortie
        globalClickCounts.toStream()
                .mapValues(count -> String.valueOf(count))
                .to("click-counts");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}