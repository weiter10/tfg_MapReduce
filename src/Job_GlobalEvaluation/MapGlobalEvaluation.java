/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_GlobalEvaluation;

import GlobalEvaluation.Example;
import GlobalEvaluation.Parse;
import GlobalEvaluation.Rule;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author manu
 */
public class MapGlobalEvaluation extends Mapper<LongWritable, Text, Text, Text>
{

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException
    {
        try
        {
            Rule best;
            Parse parse = new Parse(value.toString());
            Set<Rule> rules = parse.getRules();
            Set<Example> examples = parse.getExamples(), coveredPositives;
            PriorityQueue<Rule> orderedRules = new PriorityQueue(Collections.reverseOrder());
            
            while(!examples.isEmpty())
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
