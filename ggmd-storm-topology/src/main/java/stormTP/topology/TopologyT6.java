package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Duration; // Import direct de Duration
import org.apache.storm.tuple.Fields;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.GiveRankBolt;
import stormTP.operator.RankEvolutionBolt;
import stormTP.operator.Exit6Bolt;

public class TopologyT6 {

    public static void main(String[] args) throws Exception {

        int inputPort = (args.length > 0) ? Integer.parseInt(args[0]) : 9001;
        int outputPort = (args.length > 1) ? Integer.parseInt(args[1]) : 9002;

        TopologyBuilder topologyBuilder = new TopologyBuilder();

        topologyBuilder.setSpout("masterStream", new InputStreamSpout("127.0.0.1", inputPort));

        topologyBuilder.setBolt("rank", new GiveRankBolt(), 1)
                       .shuffleGrouping("masterStream");
        
        int idToMonitor = 2; // On surveille la tortue ID 2
        
        topologyBuilder.setBolt("evolution", 
                new RankEvolutionBolt(idToMonitor)
                    // Syntaxe correcte pour une fenêtre basée sur le temps
                    .withWindow(Duration.seconds(10), Duration.seconds(2)), 
                1)
               .fieldsGrouping("rank", new Fields("id")); 

        topologyBuilder.setBolt("exit", new Exit6Bolt(outputPort), 1)
                       .shuffleGrouping("evolution"); // Shuffle car le Bolt n'est pas "fieldsGrouping" dependent

        Config stormConfig = new Config();
        
        StormSubmitter.submitTopology("topoTP6", stormConfig, topologyBuilder.createTopology());
    }
}
