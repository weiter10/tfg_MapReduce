/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_GlobalEvaluation;

import Main_Operations.Main;
import GlobalEvaluation.Example;
import GlobalEvaluation.Parse;
import GlobalEvaluation.Rule;
import Hdfs_Operations.HdfsReader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author manu
 */
public class MapGlobalEvaluation extends Mapper<LongWritable, Text, Text, Text>
{

    /**
     * El MAP recibe los ejemplos, los mismo que en la fase de training. Tiene
     * que leer las reglas de HDFS
     * @param key
     * @param value
     * @param context
     * @throws InterruptedException
     * @throws IOException 
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException
    {
        try
        {
            //Obtenemos el buffer de lectura en HDFS para leer las reglas
            ToolRunner.run(new HdfsReader(), new String[] {"/input/" + Main.nameGlobalEvaluationFile});
            BufferedReader br = HdfsReader.br;
            
            Rule best;
            Parse parse = new Parse(value.toString(), br.readLine());
            Set<Rule> rules = parse.getRules();
            Set<Example> examples = parse.getExamples(), coveredPositives;
            PriorityQueue<Rule> orderedRules = new PriorityQueue(Collections.reverseOrder());
            
            while(!examples.isEmpty() && !rules.isEmpty())
            {
                orderedRules.addAll(rules);
                best = orderedRules.peek();
                rules.remove(best);
                coveredPositives = best.getCoveredPositives();
                examples.removeAll(coveredPositives);
                
                for(Rule r : rules)
                    r.updatePerformance(coveredPositives);
                
                orderedRules.clear();
                
                //Key: cuerpo de la regla
            
                //Value:
                //0 Clase
                //1 Num ejemplos pos
                //2 Num ejemplos neg
                //3 func. evaluzación
                //4 pi
                context.write(new Text(best.getStrBody()), new Text(best.getClassAttribute()
                        + "\t" + best.getPositives() + "\t" + best.getNegatives() +
                        "\t" + best.getEvaluationFunction() + "\t" + best.getPi()));
            }
            
        }catch (Exception ex)
        {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            context.write(new Text("Error MAP"), new Text(exceptionAsString));
        }
    }
}
