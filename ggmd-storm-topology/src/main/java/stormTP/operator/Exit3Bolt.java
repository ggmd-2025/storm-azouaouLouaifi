package stormTP.operator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class Exit3Bolt implements IRichBolt {

    private OutputCollector outputColl;
    
    // Variables pour la gestion de la Socket
    private int listenPort;
    private ServerSocket srvSocket;
    private Socket connectedClient;
    private BufferedWriter outWriter;

    public Exit3Bolt(int listenPort) {
        this.listenPort = listenPort;
    }

    @Override
    public void prepare(Map config, TopologyContext ctx, OutputCollector outputColl) {
        this.outputColl = outputColl;
        try {
            // 1. Ouverture du port serveur (une seule fois)
            this.srvSocket = new ServerSocket(this.listenPort);
            System.out.println("Exit3Bolt listening on port " + this.listenPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            // 2. Connexion au Listener si nécessaire
            if (connectedClient == null || connectedClient.isClosed()) {
                System.out.println("Waiting for listener connection...");
                connectedClient = srvSocket.accept(); // Bloquant jusqu'à connexion
                outWriter = new BufferedWriter(new OutputStreamWriter(connectedClient.getOutputStream()));
                System.out.println("Listener connected!");
            }

            // Récupération des données
            int tortoiseId = tuple.getIntegerByField("id");
            int topValue = tuple.getIntegerByField("top");
            String rank = tuple.getStringByField("rang");
            int totalValue = tuple.getIntegerByField("total");
            int maxCell = tuple.getIntegerByField("maxcel");

            // Formatage JSON
            String jsonOut = String.format(
                "{\"id\":%d,\"top\":%d,\"rang\":\"%s\",\"total\":%d,\"maxcel\":%d}",
                tortoiseId, topValue, rank, totalValue, maxCell
            );

            // 3. Envoi des données
            outWriter.write(jsonOut);
            outWriter.newLine(); // Indispensable pour le client
            outWriter.flush();
            
            System.out.println("EXIT3 >>> " + jsonOut);

            // Acquittement à Storm
            outputColl.emit(new Values(jsonOut));
            outputColl.ack(tuple);

        } catch (IOException e) {
            System.err.println("Erreur d'écriture (Listener déconnecté ?) : " + e.getMessage());
            // Réinitialisation pour permettre une reconnexion
            connectedClient = null;
            outWriter = null;
            outputColl.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (outWriter != null) outWriter.close();
            if (connectedClient != null) connectedClient.close();
            if (srvSocket != null) srvSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() { 
        return null; 
    }
}
