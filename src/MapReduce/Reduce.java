/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MapReduce;


import java.io.IOException;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, Text, Text>
{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException
    {
        try
        {
            int count = 0;

            for(Text value : values)
            {
                    count += Integer.parseInt(value.toString());
            }

            context.write(key, new Text(Integer.toString(count)));
        }
        catch(Exception ex)
        {
            context.write(new Text("Error"), values.iterator().next());
        }
    }
}
