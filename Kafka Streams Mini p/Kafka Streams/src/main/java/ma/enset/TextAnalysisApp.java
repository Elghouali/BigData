import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Properties;

public class TextAnalysisApp {

    public static void main(String[] args) {
        // 1. Configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-analysis-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // 2. Lire les messages du topic d'entrée
        KStream<String, String> sourceStream = builder.stream("text-input");

        // 3. Traitement, Filtrage et Routage
        sourceStream.split(Named.as("routing-"))
                .branch((key, originalValue) -> {
                    // A. Nettoyage temporaire pour l'évaluation
                    String cleanedValue = cleanText(originalValue);
                    // B. Vérifier si le message est valide
                    return isValid(cleanedValue);
                }, Branched.withConsumer(validStream -> validStream
                        // C. Appliquer le nettoyage final pour les messages valides
                        .mapValues(value -> cleanText(value))
                        .to("text-clean") // Envoyer vers text-clean
                ))
                .defaultBranch(Branched.withConsumer(invalidStream ->
                        // D. Les messages invalides sont envoyés "tels quels"
                        invalidStream.to("text-dead-letter")
                ));

        // 4. Démarrer l'application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // --- Méthodes Utilitaires ---

    private static String cleanText(String input) {
        if (input == null) return "";
        // Supprimer les espaces avant/après, remplacer les espaces multiples, et convertir en majuscules
        return input.trim().replaceAll("\\s+", " ").toUpperCase();
    }

    private static boolean isValid(String cleanedInput) {
        // Rejeter les messages vides ou constitués uniquement d'espaces
        if (cleanedInput.isEmpty()) return false;

        // Rejeter les messages dépassant 100 caractères
        if (cleanedInput.length() > 100) return false;

        // Rejeter les messages contenant des mots interdits
        if (cleanedInput.contains("HACK") ||
                cleanedInput.contains("SPAM") ||
                cleanedInput.contains("XXX")) {
            return false;
        }

        return true;
    }
}