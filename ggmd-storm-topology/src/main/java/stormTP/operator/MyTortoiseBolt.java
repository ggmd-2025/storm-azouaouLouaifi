package stormTP.operator;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class MyTortoiseBolt implements IRichBolt {

    private OutputCollector sortieBolt;
    private int idCible;
    private String nomTortue;

    public MyTortoiseBolt(int idCible, String nomTortue) {
        this.idCible = idCible;
        this.nomTortue = nomTortue;
    }

    @Override
    public void prepare(Map conf, TopologyContext contexte, OutputCollector sortieBolt) {
        this.sortieBolt = sortieBolt;
    }

    @Override
    public void execute(Tuple entree) {

        try {
            String jsonBrut = entree.getStringByField("json");

            int indexRunners = jsonBrut.indexOf("\"runners\":");
            if (indexRunners == -1) {
                sortieBolt.ack(entree);
                return;
            }

            int debutBloc = jsonBrut.indexOf('[', indexRunners);
            int finBloc = jsonBrut.indexOf(']', debutBloc);

            if (debutBloc == -1 || finBloc == -1) {
                sortieBolt.ack(entree);
                return;
            }

            String blocRunners = jsonBrut.substring(debutBloc + 1, finBloc);

            String[] listeTortues = blocRunners.split("\\},\\{");

            for (String tortue : listeTortues) {

                tortue = tortue.replace("{", "").replace("}", "");

                String[] champs = tortue.split(",");

                int idCourant = -1, topCourant = 0, tourCourant = 0, celluleCourante = 0, totalCourant = 0, maxCellCourante = 0;

                for (String champ : champs) {
                    String[] cleValeur = champ.split(":");

                    if (cleValeur.length < 2) continue;

                    String cle = cleValeur[0].replace("\"", "").trim();
                    String valeur = cleValeur[1].replace("\"", "").trim();

                    switch (cle) {
                        case "id": idCourant = Integer.parseInt(valeur); break;
                        case "top": topCourant = Integer.parseInt(valeur); break;
                        case "tour": tourCourant = Integer.parseInt(valeur); break;
                        case "cellule": celluleCourante = Integer.parseInt(valeur); break;
                        case "total": totalCourant = Integer.parseInt(valeur); break;
                        case "maxcel": maxCellCourante = Integer.parseInt(valeur); break;
                    }
                }

                if (idCourant == idCible) {
                    int cellulesParcourues = tourCourant * maxCellCourante + celluleCourante;

                    sortieBolt.emit(new Values(
                            idCourant, topCourant, nomTortue, cellulesParcourues, totalCourant, maxCellCourante
                    ));
                }
            }

            sortieBolt.ack(entree);

        } catch (Exception e) {
            e.printStackTrace();
            sortieBolt.fail(entree);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "id","top","nom","nbCellsParcourus","total","maxcel"
        ));
    }

    @Override
    public void cleanup() {}

    @Override
    public Map<String,Object> getComponentConfiguration(){ return null; }
}
