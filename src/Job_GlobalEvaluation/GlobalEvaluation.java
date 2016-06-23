/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_GlobalEvaluation;

import Main_Operations.Main;
import Hdfs_Operations.HdfsReaderToLocal;
import Hdfs_Operations.HdfsRemove;
import Job_Training.MapTraining;
import Job_Training.ReduceTraining;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author manu
 */
public class GlobalEvaluation extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        
        conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit");

        Job job = new Job(conf, "GlobalEvaluation");

        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapGlobalEvaluation.class);
        job.setReducerClass(ReduceTraining.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path p = new Path(Main.pathFolderTraining);
        MultipleInputs.addInputPath(job, p, TextInputFormat.class, MapGlobalEvaluation.class);
        FileOutputFormat.setOutputPath(job, new Path("/output"));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception
    {
        String[] args2 = new String[2];
        String nameFileOutputMR = "/output/part-r-00000", str = "";
        int i = 1;

        //Borramos el directorio de salida de trabajos MapReduce
        args2[0] = "/output";
        args2[1] = "";
        ToolRunner.run(new HdfsRemove(), args2);

        //Lanzamos la evaluación global
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting GlobalEvaluation_" + i);
        ToolRunner.run(new GlobalEvaluation(), args);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> GlobalEvaluation_" + i + " OK");

        //Escribimos el resultado en el almacenamiento local
        args2[0] = nameFileOutputMR;
        args2[1] = args[4] + "/GlobalEvaluationOutput_" + i;
        ToolRunner.run(new HdfsReaderToLocal(), args2);
    }
}
