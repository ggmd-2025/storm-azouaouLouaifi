package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.PointsBolt;
import stormTP.operator.Exit4Bolt;

public class TopologyT4 {

    public static void main(String[] args) throws Exception {

        int inputPort = Integer.parseInt(args[0]);
        int outputPort = Integer.parseInt(args[1]);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 1. Spout
        topologyBuilder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", inputPort));

        // 2. Calcul du Rang
        topologyBuilder.setBolt("rank", new GiveRankBolt(), 1)
                       .shuffleGrouping("masterStream");
        
        // 3. Calcul des Points (reçoit le flux de "rank")
        topologyBuilder.setBolt("points", new PointsBolt(), 1)
                       .shuffleGrouping("rank");

        // 4. Sortie (reçoit le flux de "points")
        topologyBuilder.setBolt("exit", new Exit4Bolt(outputPort), 1)
                       .shuffleGrouping("points");

        Config stormConfig = new Config();
        StormSubmitter.submitTopology("topoTP4", stormConfig, topologyBuilder.createTopology());
    }
}
