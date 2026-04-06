package ma.enset;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.*;
import static org.apache.spark.sql.functions.*;

public class StreamingCapteursHDFS {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder().appName("TP_Streaming_HDFS").getOrCreate();

        // --- RÉPONSE Q4 : Schéma explicite et lecture depuis HDFS ---
        StructType schemaCapteur = new StructType()
                .add("id", DataTypes.IntegerType)
                .add("timestamp", DataTypes.TimestampType)
                .add("capteur", DataTypes.StringType)
                .add("valeur", DataTypes.DoubleType)
                .add("unite", DataTypes.StringType);

        Dataset<Row> fluxCapteurs = spark.readStream()
                .option("sep", ",").option("header", "true").schema(schemaCapteur)
                .csv("hdfs://localhost:9000/streaming/capteurs");

        // --- RÉPONSE Q5 : Calcul de la moyenne, min, max et count ---
        Dataset<Row> statsCapteurs = fluxCapteurs.groupBy("capteur")
                .agg(
                        avg("valeur").as("moyenne"),
                        min("valeur").as("minimum"),
                        max("valeur").as("maximum"),
                        count("id").as("nb_mesures")
                );

        // --- RÉPONSE Q6 : Identifier les anomalies (ex: valeur > 30) ---
        Dataset<Row> anomalies = fluxCapteurs.filter("valeur > 30.0")
                .withColumn("statut", lit("ANOMALIE_DETECTEE"));

        // Démarrage des flux (Console)
        StreamingQuery qStats = statsCapteurs.writeStream().outputMode("complete").format("console")
                .option("checkpointLocation", "hdfs://localhost:9000/streaming/checkpoints/stats").start();

        StreamingQuery qAnomalies = anomalies.writeStream().outputMode("append").format("console")
                .option("checkpointLocation", "hdfs://localhost:9000/streaming/checkpoints/anomalies").start();

        qStats.awaitTermination();
        qAnomalies.awaitTermination();
    }
}