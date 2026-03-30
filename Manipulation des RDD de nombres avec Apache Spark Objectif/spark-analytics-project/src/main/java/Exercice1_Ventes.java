package ma.enset.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class Exercice1_Ventes {
    public static void main(String[] args) {
        // 1. Configuration (Test en local)
        SparkConf conf = new SparkConf().setAppName("Analyse Ventes").setMaster("local[*]");

        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            // Désactiver les logs INFO de Spark pour garder la console propre
            sc.setLogLevel("ERROR");

            // Chargement du fichier
            JavaRDD<String> lignes = sc.textFile("data/ventes.txt");

            System.out.println("=== 1. Total des ventes par ville ===");
            JavaPairRDD<String, Double> ventesParVille = lignes.mapToPair(ligne -> {
                String[] mots = ligne.split(" ");
                // Index : 0=date, 1=ville, 2=produit, 3=prix
                return new Tuple2<>(mots[1], Double.parseDouble(mots[3]));
            }).reduceByKey(Double::sum); // Somme par clé (ville)

            ventesParVille.foreach(tuple -> System.out.println(tuple._1 + " : " + tuple._2 + " DH"));

            System.out.println("\n=== 2. Prix total des ventes par ville et par année ===");
            JavaPairRDD<Tuple2<String, String>, Double> ventesVilleAnnee = lignes.mapToPair(ligne -> {
                String[] mots = ligne.split(" ");
                String annee = mots[0].split("-")[0]; // Extraction de l'année (ex: 2024-05-12 -> 2024)
                String ville = mots[1];
                Double prix = Double.parseDouble(mots[3]);

                // La clé est maintenant un Tuple composé de (Ville, Année)
                return new Tuple2<>(new Tuple2<>(ville, annee), prix);
            }).reduceByKey(Double::sum);

            ventesVilleAnnee.foreach(tuple ->
                    System.out.println("Ville: " + tuple._1._1 + " | Année: " + tuple._1._2 + " -> Total: " + tuple._2)
            );
        }
    }
}