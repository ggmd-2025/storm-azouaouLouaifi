package stormTP.operator;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpeedBolt extends BaseWindowedBolt {

    private OutputCollector collectorInstance;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.collectorInstance = collector;
    }

    @Override
    public void execute(TupleWindow window) {
        List<Tuple> tupleList = window.get();
        
        if (tupleList.size() < 2) return;

        Tuple firstTuple = tupleList.get(0);
        Map<Integer, RunnerInfo> initialRunners = parseRunners(firstTuple.getStringByField("json"));

        Tuple lastTuple = tupleList.get(tupleList.size() - 1);
        Map<Integer, RunnerInfo> finalRunners = parseRunners(lastTuple.getStringByField("json"));

        for (Integer runnerId : initialRunners.keySet()) {
            if (finalRunners.containsKey(runnerId)) {
                RunnerInfo startRunner = initialRunners.get(runnerId);
                RunnerInfo endRunner = finalRunners.get(runnerId);

                long startDistance = (long) startRunner.lap * startRunner.maxCell + startRunner.cell;
                long endDistance = (long) endRunner.lap * endRunner.maxCell + endRunner.cell;
                long traveledDistance = endDistance - startDistance;

                long elapsedTime = endRunner.time - startRunner.time;

                double speed = 0.0;
                if (elapsedTime > 0) {
                    speed = (double) traveledDistance / elapsedTime;
                }

                String timeRange = startRunner.time + "-" + endRunner.time;

                collectorInstance.emit(new Values(runnerId, startRunner.name, timeRange, speed));
            }
        }
    }

    private Map<Integer, RunnerInfo> parseRunners(String jsonString) {
        Map<Integer, RunnerInfo> runnerMap = new HashMap<>();
        try {
            JsonReader reader = Json.createReader(new StringReader(jsonString));
            JsonObject rootObj = reader.readObject();
            
            String arrayKey = rootObj.containsKey("tortoises") ? "tortoises" : "runners";
            if (rootObj.containsKey(arrayKey)) {
                JsonArray runnersArray = rootObj.getJsonArray(arrayKey);
                for (JsonValue val : runnersArray) {
                    JsonObject runnerObj = (JsonObject) val;
                    RunnerInfo info = new RunnerInfo();
                    info.id = runnerObj.getInt("id");
                    info.time = runnerObj.containsKey("top") ? runnerObj.getInt("top") : 0;
                    info.lap = runnerObj.containsKey("tour") ? runnerObj.getInt("tour") : 0;
                    info.cell = runnerObj.containsKey("cellule") ? runnerObj.getInt("cellule") : 0;
                    info.maxCell = runnerObj.containsKey("maxcel") ? runnerObj.getInt("maxcel") : 150;
                    info.name = runnerObj.containsKey("nom") ? runnerObj.getString("nom") : "Runner" + info.id;
                    
                    runnerMap.put(info.id, info);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return runnerMap;
    }

    class RunnerInfo {
        int id;
        long time;
        int lap;
        int cell;
        int maxCell;
        String name;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "name", "timeRange", "speed"));
    }
}
