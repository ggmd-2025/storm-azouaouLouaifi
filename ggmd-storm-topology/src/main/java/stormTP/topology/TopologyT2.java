package stormTP.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import stormTP.operator.InputStreamSpout;
import stormTP.operator.MyTortoiseBolt;
import stormTP.operator.Exit2Bolt;

public class TopologyT2 {

    public static void main(String[] args) throws Exception {

        int nbExec = 1;

        if (args.length != 2) {
            System.out.println("Usage: TopologyT2 <portInput> <portOutput>");
            return;
        }

        int inputPort = Integer.parseInt(args[0]);
        int outputPort = Integer.parseInt(args[1]);

        InputStreamSpout inputSpout = new InputStreamSpout("127.0.0.1", inputPort);

        TopologyBuilder topoBuilder = new TopologyBuilder();
        topoBuilder.setSpout("masterStream", inputSpout);

        topoBuilder.setBolt("filterTortoise",
                new MyTortoiseBolt(3, "Raphaelo"), nbExec)
                .shuffleGrouping("masterStream");

        topoBuilder.setBolt("exit",
                new Exit2Bolt(outputPort), nbExec)
                .shuffleGrouping("filterTortoise");

        Config conf = new Config();

        // --- SOUMISSION VIA STORM CLI ---
        StormSubmitter.submitTopology("topoTP2", conf, topoBuilder.createTopology());
    }
}
