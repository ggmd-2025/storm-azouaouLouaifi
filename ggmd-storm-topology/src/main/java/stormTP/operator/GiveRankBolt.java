package stormTP.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class GiveRankBolt implements IRichBolt {

    private OutputCollector outCollector;

    @Override
    public void prepare(Map config, TopologyContext ctx, OutputCollector outCollector) {
        this.outCollector = outCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String rawJson = tuple.getStringByField("json");

            // Extraction du bloc "runners" ou "tortoises"
            int blockIndex = rawJson.indexOf("\"runners\":[");
            if (blockIndex < 0) blockIndex = rawJson.indexOf("\"tortoises\":[");

            if (blockIndex < 0) {
                outCollector.ack(tuple);
                return;
            }

            int startIdx = blockIndex + rawJson.substring(blockIndex).indexOf("[");
            int endIdx = rawJson.lastIndexOf("]");
            String runnersSegment = rawJson.substring(startIdx + 1, endIdx);

            // Découpage manuel des objets JSON
            List<String> tortoiseJsonList = new ArrayList<>();
            StringBuilder sb = new StringBuilder();
            int braceCount = 0;
            for (char c : runnersSegment.toCharArray()) {
                if (c == '{') braceCount++;
                if (braceCount > 0) sb.append(c);
                if (c == '}') {
                    braceCount--;
                    if (braceCount == 0) {
                        tortoiseJsonList.add(sb.toString());
                        sb = new StringBuilder();
                    }
                }
            }

            // Classe interne temporaire
            class RunnerData {
                int id, top, total, maxcel, nbCells;
                String name;
            }

            List<RunnerData> parsedList = new ArrayList<>();

            for (String obj : tortoiseJsonList) {
                RunnerData r = new RunnerData();
                r.id = extractInt(obj, "\"id\":");
                r.top = extractInt(obj, "\"top\":");
                r.total = extractInt(obj, "\"total\":");
                r.maxcel = extractInt(obj, "\"maxcel\":");

                r.name = extractString(obj, "\"nom\":");

                int lap = extractInt(obj, "\"tour\":");
                int cell = extractInt(obj, "\"cellule\":");
                r.nbCells = lap * r.maxcel + cell;

                parsedList.add(r);
            }

            // Tri décroissant
            parsedList.sort((a, b) -> Integer.compare(b.nbCells, a.nbCells));

            // Calcul et émission
            int rankCounter = 1;
            for (int i = 0; i < parsedList.size(); i++) {
                RunnerData r = parsedList.get(i);
                String rankValue;

                if (i > 0 && parsedList.get(i).nbCells == parsedList.get(i - 1).nbCells) {
                    rankValue = rankCounter + "ex";
                } else {
                    if (i > 0) rankCounter = i + 1;
                    rankValue = String.valueOf(rankCounter);
                }

                outCollector.emit(new Values(r.id, r.top, r.name, rankValue, r.total, r.maxcel));
            }

            outCollector.ack(tuple);

        } catch (Exception e) {
            e.printStackTrace();
            outCollector.fail(tuple);
        }
    }

    private int extractInt(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) return 0;
        i += key.length();
        int j = i;
        while (j < json.length() && (Character.isDigit(json.charAt(j)) || json.charAt(j) == '-')) j++;
        try { return Integer.parseInt(json.substring(i, j)); } catch(Exception e) { return 0; }
    }

    private String extractString(String json, String key) {
        int i = json.indexOf(key);
        if (i < 0) return "";
        i += key.length();
        if (i < json.length() && json.charAt(i) == '"') i++;
        int j = json.indexOf('"', i);
        if (j < 0) return "";
        return json.substring(i, j);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "top", "nom", "rang", "total", "maxcel"));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String, Object> getComponentConfiguration() { 
        return null; 
    }
}
