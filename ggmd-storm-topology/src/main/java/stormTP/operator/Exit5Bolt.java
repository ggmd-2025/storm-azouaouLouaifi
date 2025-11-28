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

public class Exit5Bolt implements IRichBolt {

    private OutputCollector outCollector;
    private int listenPort;
    private ServerSocket srvSocket;
    private Socket clientConn;
    private BufferedWriter outWriter;

    public Exit5Bolt(int listenPort) {
        this.listenPort = listenPort;
    }

    @Override
    public void prepare(Map config, TopologyContext ctx, OutputCollector outCollector) {
        this.outCollector = outCollector;
        try {
            this.srvSocket = new ServerSocket(this.listenPort);
            System.out.println("Exit5Bolt listening on port " + this.listenPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            if (clientConn == null || clientConn.isClosed()) {
                clientConn = srvSocket.accept();
                outWriter = new BufferedWriter(new OutputStreamWriter(clientConn.getOutputStream()));
            }

            // Récupération des données envoyées par SpeedBolt
            int tortoiseId = tuple.getIntegerByField("id");
            String name = tuple.getStringByField("nom");
            String topsValue = tuple.getStringByField("tops");
            double speed = tuple.getDoubleByField("vitesse");

            // Construction du JSON attendu
            String jsonOut = String.format(
                "{\"id\":%d,\"nom\":\"%s\",\"tops\":\"%s\",\"vitesse\":%.2f}",
                tortoiseId, name, topsValue, speed
            );

            outWriter.write(jsonOut);
            outWriter.newLine();
            outWriter.flush();
            
            System.out.println("EXIT5 >>> " + jsonOut);

            outCollector.emit(new Values(jsonOut));
            outCollector.ack(tuple);

        } catch (IOException e) {
            System.err.println("Erreur Exit5Bolt : " + e.getMessage());
            clientConn = null;
            outWriter = null;
            outCollector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {
        try {
            if (outWriter != null) outWriter.close();
            if (clientConn != null) clientConn.close();
            if (srvSocket != null) srvSocket.close();
        } catch (IOException e) {}
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
