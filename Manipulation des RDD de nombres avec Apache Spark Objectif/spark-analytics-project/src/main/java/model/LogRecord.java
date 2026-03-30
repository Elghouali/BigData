package ma.enset.bigdata.spark.model;

import java.io.Serializable;

// Implémente Serializable car les objets vont transiter sur le réseau (cluster)
public class LogRecord implements Serializable {
    public String ip;
    public String date;
    public String methode;
    public String ressource;
    public int codeHttp;
    public long taille;

    public LogRecord(String ip, String date, String methode, String ressource, int codeHttp, long taille) {
        this.ip = ip;
        this.date = date;
        this.methode = methode;
        this.ressource = ressource;
        this.codeHttp = codeHttp;
        this.taille = taille;
    }
}