package ma.enset.bigdata.spark;

import ma.enset.bigdata.spark.model.LogRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Exercice2_Logs {
    // Regex pour parser le format de log Apache fourni
    private static final String LOG_PATTERN = "^(\\S+) \\S+ \\S+ \\[(.*?)\\] \"(\\S+) (\\S+) .*?\" (\\d{3}) (\\d+)";
    private static final Pattern PATTERN = Pattern.compile(LOG_PATTERN);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Analyse Logs Apache").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            sc.setLogLevel("ERROR");

            // 1. Lecture des données
            JavaRDD<String> lignes = sc.textFile("data/access.log");

            // 2. Extraction des champs vers des objets LogRecord
            JavaRDD<LogRecord> logsRdd = lignes.mapPartitions(iterator -> {
                java.util.List<LogRecord> logsList = new java.util.ArrayList<>();
                while (iterator.hasNext()) {
                    String ligne = iterator.next();
                    Matcher matcher = PATTERN.matcher(ligne);
                    if (matcher.find()) {
                        logsList.add(new LogRecord(
                                matcher.group(1), // IP
                                matcher.group(2), // Date
                                matcher.group(3), // Méthode HTTP
                                matcher.group(4), // Ressource
                                Integer.parseInt(matcher.group(5)), // Code
                                Long.parseLong(matcher.group(6))    // Taille
                        ));
                    }
                }
                return logsList.iterator();
            });

            // Action pour forcer l'évaluation en mémoire (optimisation si le RDD est réutilisé)
            logsRdd.cache();

            // 3. Statistiques de base
            long totalRequetes = logsRdd.count();
            long totalErreurs = logsRdd.filter(log -> log.codeHttp >= 400).count();
            double pourcentageErreurs = ((double) totalErreurs / totalRequetes) * 100;

            System.out.println("=== 3. Statistiques de base ===");
            System.out.println("Total requêtes : " + totalRequetes);
            System.out.println("Total erreurs (>=400) : " + totalErreurs);
            System.out.printf("Pourcentage d'erreurs : %.2f%%\n\n", pourcentageErreurs);

            // 4. Top 5 des adresses IP
            System.out.println("=== 4. Top 5 des adresses IP ===");
            List<Tuple2<Integer, String>> topIps = logsRdd
                    .mapToPair(log -> new Tuple2<>(log.ip, 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(Tuple2::swap) // Inverse pour trier par valeur : (Compte, IP)
                    .sortByKey(false)        // Tri décroissant
                    .take(5);
            topIps.forEach(t -> System.out.println("IP: " + t._2 + " -> " + t._1 + " requêtes"));

            // 5. Top 5 des ressources les plus demandées
            System.out.println("\n=== 5. Top 5 des ressources ===");
            List<Tuple2<Integer, String>> topRessources = logsRdd
                    .mapToPair(log -> new Tuple2<>(log.ressource, 1))
                    .reduceByKey(Integer::sum)
                    .mapToPair(Tuple2::swap)
                    .sortByKey(false)
                    .take(5);
            topRessources.forEach(t -> System.out.println("Ressource: " + t._2 + " -> " + t._1 + " fois"));

            // 6. Répartition des requêtes par code HTTP
            System.out.println("\n=== 6. Répartition par code HTTP ===");
            JavaPairRDD<Integer, Integer> repartitionCodes = logsRdd
                    .mapToPair(log -> new Tuple2<>(log.codeHttp, 1))
                    .reduceByKey(Integer::sum);
            repartitionCodes.collect().forEach(t -> System.out.println("Code HTTP " + t._1 + " : " + t._2 + " requêtes"));
        }
    }
}