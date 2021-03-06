/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Training;


import Job_Training.InfoRule;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceTraining extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException
    {
        if(key.toString().equals("Error MAP"))
            for(Text t : values) context.write(key, t);
        
        else
        {
            try
            {
                //Clave: clase, valor: información acumulada para esa clase
                Map<String,InfoRule> infoMap = new HashMap();
                InfoRule max = null;

                for(Text value : values)
                {
                    String[] param = value.toString().split("\t");

                    if(!infoMap.containsKey(param[0]))
                        infoMap.put(param[0], new InfoRule(param[0], key.toString()));

                    InfoRule ir = infoMap.get(param[0]);
                    ir.addPositives(Integer.parseInt(param[1]));
                    ir.addNegatives(Integer.parseInt(param[2]));
                    ir.increaseCount();
                }

                ArrayList<InfoRule> info = new ArrayList(infoMap.values());

                for(InfoRule in : info)
                    in.calculatePi();

                if(info.size() > 1)
                {
                    InfoRule r1 = info.get(0);
                    InfoRule r2 = info.get(1);


                    if(r1.getPi() > r2.getPi()) max = r1;

                    else max = r2;
                }
                else
                {
                    max = info.get(0);
                }
                /*
                key: regla
                value:
                1 nº de apariciones de la regla
                2 nº positivos
                3 nº negativos
                4 pi'
                */
                context.write(new Text(key.toString() + max.getClassRule()), new Text(
                        "\t" + Integer.toString(max.getCount()) + "\t" + max.getPositives() + 
                        "\t" + max.getNegatives() + "\t" + max.getPi()));

            }catch (Exception ex)
            {
                StringWriter sw = new StringWriter();
                ex.printStackTrace(new PrintWriter(sw));
                String exceptionAsString = sw.toString();
                context.write(new Text("Error REDUCE"), new Text(exceptionAsString));
            }
        }
    }
}
