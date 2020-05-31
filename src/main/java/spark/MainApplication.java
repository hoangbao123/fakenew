package spark;

public class MainApplication {
    public static void main(String[] args) throws InterruptedException {
        SparkConnector sparkConnector = new SparkConnector();
        sparkConnector.initKafkaReceviver();
    }
}
