package stormTP.operator;

import java.util.Map;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PointsBolt implements IRichBolt {

    private OutputCollector sortie;

    @Override
    public void prepare(Map conf, TopologyContext contexte, OutputCollector sortie) {
        this.sortie = sortie;
    }

    @Override
    public void execute(Tuple tupleEntree) {
        try {
            int identifiant = tupleEntree.getIntegerByField("id");
            int classementTop = tupleEntree.getIntegerByField("top");
            String rangTexte = tupleEntree.getStringByField("rang");
            int totalCellules = tupleEntree.getIntegerByField("total");
            int maxCellule = tupleEntree.getIntegerByField("maxcel");

            int rangNum = Integer.parseInt(rangTexte.replace("ex", ""));

            int score = 0;
            if (rangNum > 0) {
                score = 100 / rangNum;
            }

            sortie.emit(tupleEntree, new Values(
                    identifiant, classementTop, rangTexte, totalCellules, maxCellule, score
            ));
            sortie.ack(tupleEntree);

        } catch (Exception e) {
            e.printStackTrace();
            sortie.fail(tupleEntree);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "rang", "total", "maxcel", "points"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}
