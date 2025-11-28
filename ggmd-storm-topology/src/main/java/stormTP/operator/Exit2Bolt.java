package stormTP.operator;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import stormTP.stream.StreamEmiter;

public class Exit2Bolt implements IRichBolt {

    private OutputCollector outCollector;
    private int outputPort = -1;
    private StreamEmiter streamEmitter = null;

    public Exit2Bolt(int outputPort) {
        this.outputPort = outputPort;
        this.streamEmitter = new StreamEmiter(this.outputPort);
    }

    @Override
    public void prepare(Map config, TopologyContext ctx, OutputCollector outCollector) {
        this.outCollector = outCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        int tortoiseId = tuple.getIntegerByField("id");
        int topValue = tuple.getIntegerByField("top");
        String name = tuple.getStringByField("nom");
        int cellsDone = tuple.getIntegerByField("nbCellsParcourus");
        int totalValue = tuple.getIntegerByField("total");
        int maxCell = tuple.getIntegerByField("maxcel");

        // Construction du JSON attendu
        String jsonOutput = String.format(
                "{\"id\":%d,\"top\":%d,\"nom\":\"%s\",\"nbCellsParcourus\":%d,\"total\":%d,\"maxcel\":%d}",
                tortoiseId, topValue, name, cellsDone, totalValue, maxCell
        );

        // Envoi via StreamEmiter
        this.streamEmitter.send(jsonOutput);

        // On émet également le champ pour compatibilité Storm
        outCollector.emit(new Values(jsonOutput));

        outCollector.ack(tuple);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("json"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
