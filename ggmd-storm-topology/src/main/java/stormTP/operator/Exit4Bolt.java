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

public class Exit4Bolt implements IRichBolt {

    private OutputCollector outCollector;
    private int listenPort;
    private ServerSocket srvSocket;
    private Socket clientConn;
    private BufferedWriter outWriter;

    public Exit4Bolt(int listenPort) {
        this.listenPort = listenPort;
    }

    @Override
    public void prepare(Map config, TopologyContext ctx, OutputCollector outCollector) {
        this.outCollector = outCollector;
        try {
            this.srvSocket = new ServerSocket(this.listenPort);
            System.out.println("Exit4Bolt listening on port " + this.listenPort);
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

            // Récupération des données incluant les points
            int tortoiseId = tuple.getIntegerByField("id");
            int topValue = tuple.getIntegerByField("top");
            String rank = tuple.getStringByField("rang");
            int totalValue = tuple.getIntegerByField("total");
            int maxCell = tuple.getIntegerByField("maxcel");
            int scorePoints = tuple.getIntegerByField("points");

            // Construction du JSON avec le champ points
            String jsonOutput = String.format(
                "{\"id\":%d,\"top\":%d,\"rang\":\"%s\",\"points\":%d,\"total\":%d,\"maxcel\":%d}",
                tortoiseId, topValue, rank, scorePoints, totalValue, maxCell
            );

            outWriter.write(jsonOutput);
            outWriter.newLine();
            outWriter.flush();
            
            System.out.println("EXIT4 >>> " + jsonOutput);

            outCollector.emit(new Values(jsonOutput));
            outCollector.ack(tuple);

        } catch (IOException e) {
            System.err.println("Erreur Exit4Bolt : " + e.getMessage());
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
