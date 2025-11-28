package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.Exit3Bolt;

public class TopologyT3 {

    public static void main(String[] args) throws Exception {

        int inputPort = Integer.parseInt(args[0]);
        int outputPort = Integer.parseInt(args[1]);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", inputPort));

        topologyBuilder.setBolt("rank", new GiveRankBolt(), 1)
                       .shuffleGrouping("masterStream");

        topologyBuilder.setBolt("exit", new Exit3Bolt(outputPort), 1)
                       .allGrouping("rank");     // <---- LA SEULE LIGNE IMPORTANTE

        Config stormConfig = new Config();

        StormSubmitter.submitTopology("topoTP3", stormConfig, topologyBuilder.createTopology());
    }
}
