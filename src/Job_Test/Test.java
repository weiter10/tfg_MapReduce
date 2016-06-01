/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Test;

import Driver_Operations.Driver;
import static Driver_Operations.Driver.pathFolderOutputMR;
import Hdfs_Operations.HdfsReaderToLocal;
import Hdfs_Operations.HdfsRemove;
import Job_Training.MapTraining;
import Job_Training.Training;
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
public class Test extends Configured implements Tool
{
    @Override
    public int run(String[] strings) throws Exception
    {
        Configuration conf = getConf();
        
        conf.set("mapred.child.java.opts", "-XX:-UseGCOverheadLimit");
        
        Job job = new Job(conf, "TestFase");
        
        job.setJarByClass(Driver.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MapTest.class);
        job.setReducerClass(ReduceTest.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        Path p = new Path(Driver.pathFolderTestFile);
        MultipleInputs.addInputPath(job, p, TextInputFormat.class, MapTest.class);
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        
        job.waitForCompletion(true);
        
        return 0;
    }
    
    public static void main(String[] args) throws Exception
    {
        String[] args2 = new String[2];
        String nameFileOutputMR = "/output/part-r-00000", str = "";
        int i = 1;

        if (args.length < 4)
        {
            System.err.println("Number of arguments incorrect");
            System.err.println("Local_dataset_name Name_positive_class Num_colum_positive_class"
                    + "File_output_name");
            System.exit(1);
        }

        //Borramos el directorio de salida de trabajos MapReduce
        args2[0] = pathFolderOutputMR;
        args2[1] = "";
        ToolRunner.run(new HdfsRemove(), args2);

        //Lanzamos el MR que determinará la precisión del clasificador
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Test_" + i);
        ToolRunner.run(new Test(), args);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Test_" + i + " OK");

        //Escribimos el resultado en el almacenamiento local
        args2[0] = nameFileOutputMR;
        args2[1] = args[4] + "/Test_" + i;
        ToolRunner.run(new HdfsReaderToLocal(), args2);
    }
}
