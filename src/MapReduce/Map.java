/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MapReduce;

import AGL.Algorithm;
import AGL.Parse;
import AGL.Rule;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
public class Map extends Mapper<LongWritable, Text, Text, Text>
{
    private BufferedReader br = null;
    
    public class HdfsReaderMap extends Configured implements Tool
    {
        public static final String FS_PARAM_NAME = "fs.defaultFS";

        @Override
        public int run(String[] args) throws Exception
        {
            Path inputPath = new Path(args[0]);
            Configuration conf = getConf();
            FileSystem fs = FileSystem.get(conf);

            br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
            
            return 0;//ok
        }
    }
    
    
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException
    {
        try
        {
            Random rnd = new Random();
            rnd.setSeed(0);
            Algorithm alg = new Algorithm(value.toString(),rnd);
            Set<Rule> finalRules = alg.run();
            
            for(Rule r : finalRules)
                context.write(new Text(r.getStrBody()), new Text(r.getClassAttribute()
                        + "\t" + r.getPositives() + "\t" + r.getNegatives() +
                        "\t" + r.getEvaluationFunction() + "\t" + r.getPi()));
            
            //Key: cuerpo de la regla
            
            //Value:
            //0 Clase
            //1 Num ejemplos pos
            //2 Num ejemplos neg
            //3 func. evaluzación
            //4 pi
            
        }catch (Exception ex)
        {
            context.write(new Text("Error"), new Text(ex.getMessage() + "\n" + 
                    ex.getLocalizedMessage() + "\n" + ex.toString()));
        }
    }
}
