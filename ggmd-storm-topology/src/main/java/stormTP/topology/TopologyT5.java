package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.SpeedBolt;
import stormTP.operator.Exit5Bolt;

public class TopologyT5 {

    public static void main(String[] args) throws Exception {

        int inputPort = Integer.parseInt(args[0]);
        int outputPort = Integer.parseInt(args[1]);

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // 1. Source
        topologyBuilder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", inputPort));

        // 2. Calcul de la Vitesse avec fenêtrage
        // Fenêtre de longueur 10 (Count.of(10)) qui avance de 5 (Count.of(5))
        topologyBuilder.setBolt("speed", 
                new SpeedBolt()
                    .withWindow(BaseWindowedBolt.Count.of(10), BaseWindowedBolt.Count.of(5)), 
                1)
               .shuffleGrouping("masterStream");

        // 3. Sortie
        topologyBuilder.setBolt("exit", new Exit5Bolt(outputPort), 1)
               .shuffleGrouping("speed");

        Config stormConfig = new Config();
        StormSubmitter.submitTopology("topoTP5", stormConfig, topologyBuilder.createTopology());
    }
}
