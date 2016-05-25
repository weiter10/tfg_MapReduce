/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Training;

import Driver_Operations.Driver;
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
public class Training extends Configured implements Tool
{
    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", 
                //Long.toString(Driver.sizeTrainingSet/Driver.numMaps));
                
        //conf.set("mapreduce.input.fileinputformat.split.maxsize", 
                //Long.toString(Driver.sizeTrainingSetFile/13));
                
        conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit");
        
        Job job = new Job(conf, "DGA");
        
        job.setJarByClass(Driver.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapTraining.class);
        job.setReducerClass(ReduceTraining.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/input/" + Driver.nameTrainingSetFile));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        
        job.waitForCompletion(true);
        
        return 0;
    }
}
