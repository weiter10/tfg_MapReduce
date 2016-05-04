/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Driver_Operations;

import MapReduce.Map1;
import MapReduce.Map2;
import MapReduce.Reduce1;
import MapReduce.Reduce2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 *
 * @author manu
 */
public class Job2 extends Configured implements Tool
{
    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        
        Job job = new Job(conf, "TestFase");
        
        job.setJarByClass(Driver.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map2.class);
        job.setReducerClass(Reduce2.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/input/testset"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        
        job.waitForCompletion(true);
        
        return 0;
    }
}