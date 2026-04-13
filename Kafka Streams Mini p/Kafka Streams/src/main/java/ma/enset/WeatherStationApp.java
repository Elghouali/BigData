import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Properties;

public class WeatherStationApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analytics-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // Disable cache for instant testing

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Lire les données
        KStream<String, String> weatherStream = builder.stream("weather-data");

        // 2. Filtrer: Uniquement > 30°C
        KStream<String, String> filteredStream = weatherStream.filter((key, value) -> {
            String[] parts = value.split(",");
            if (parts.length < 3) return false;
            try {
                double tempC = Double.parseDouble(parts[1]);
                return tempC > 30.0;
            } catch (NumberFormatException e) {
                return false;
            }
        });

        // 3. Map: Extraire la station comme clé et convertir en Fahrenheit
        KStream<String, String> mappedStream = filteredStream.map((key, value) -> {
            String[] parts = value.split(",");
            String station = parts[0];
            double tempC = Double.parseDouble(parts[1]);
            double humidity = Double.parseDouble(parts[2]);

            // Formule: F = (C * 9/5) + 32
            double tempF = (tempC * 9.0 / 5.0) + 32.0;

            // La nouvelle valeur est "tempF,humidité"
            return KeyValue.pair(station, tempF + "," + humidity);
        });

        // 4. Grouper par station
        KGroupedStream<String, String> groupedStream = mappedStream.groupByKey();

        // 5. Agréger (Stocker l'état sous forme de "count,sumTemp,sumHumid")
        KTable<String, String> aggregatedTable = groupedStream.aggregate(
                () -> "0,0.0,0.0", // État initial
                (aggKey, newValue, aggValue) -> {
                    // Extraire les nouvelles valeurs
                    String[] newParts = newValue.split(",");
                    double newTempF = Double.parseDouble(newParts[0]);
                    double newHumid = Double.parseDouble(newParts[1]);

                    // Extraire l'état actuel
                    String[] aggParts = aggValue.split(",");
                    int count = Integer.parseInt(aggParts[0]);
                    double sumTempF = Double.parseDouble(aggParts[1]);
                    double sumHumid = Double.parseDouble(aggParts[2]);

                    // Mettre à jour l'état
                    count++;
                    sumTempF += newTempF;
                    sumHumid += newHumid;

                    return count + "," + sumTempF + "," + sumHumid;
                },
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // 6. Calculer la moyenne finale pour l'affichage
        KTable<String, String> finalAverages = aggregatedTable.mapValues(value -> {
            String[] parts = value.split(",");
            int count = Integer.parseInt(parts[0]);
            double sumTemp = Double.parseDouble(parts[1]);
            double sumHumid = Double.parseDouble(parts[2]);

            double avgTemp = sumTemp / count;
            double avgHumid = sumHumid / count;

            return "Température Moyenne = " + String.format("%.2f", avgTemp) + "°F Humidité Moyenne = " + String.format("%.2f", avgHumid) + "%";
        });

        // 7. Écrire les résultats
        finalAverages.toStream().to("station-averages");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}