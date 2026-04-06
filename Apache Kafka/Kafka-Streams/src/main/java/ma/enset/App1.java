import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import java.util.Properties;

public class App1 {
    public static void main(String[] args) {
        // 1. Configure the Kafka Streams application
        Properties props = new Properties();
        props.put("application.id", "kafka-streams-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");
        props.put("default.value.serde", "org.apache.kafka.common.serialization.Serdes$StringSerde");

        // 2. Build the Stream Topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("input-topic");

        // Transformation: convert to uppercase and append a suffix
        KStream<String, String> upperCaseStream = sourceStream.mapValues(value -> value.toUpperCase() + "-TEST");
        upperCaseStream.to("output-topic");

        // 3. Start the Kafka Streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // 4. Add a shutdown hook for a clean exit
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}