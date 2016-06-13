/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Test;


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

public class ReduceTest extends Reducer<Text, Text, Text, Text>
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
                long count = 0;

                for(Text value : values)
                {
                    count += Long.parseLong(value.toString());
                }
                
                //Key: caso (positivos o negativos)
            
                //Value:
                //nº de ejemplos cubiertos
                context.write(key, new Text(Long.toString(count)));

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
