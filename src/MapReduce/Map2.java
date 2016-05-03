/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MapReduce;

import TestFase.Example;
import TestFase.Parse;
import TestFase.Rule;
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
public class Map2 extends Mapper<LongWritable, Text, Text, Text>
{

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException
    {
        try
        {
            long numPos = 0, numNeg = 0;
            int result, index;
            String defaultClass = "10";
            String[] data = value.toString().split("\t\t");
            //Cargamos las reglas
            ArrayList<Rule> rules = Parse.parseRules(data[0]);
            Example ex;
            
            for(String exampleStr : data[1].split("\t"))
            {
                result = 0;
                index = 0;
                ex = Parse.parseExample(exampleStr);
                
                while(index < rules.size() && result == 0)
                {
                    result = rules.get(index).coverExample(ex);
                    index++;
                }
                
                //Lo cubre correctamente
                if(result == 1)
                    numPos++;
                
                //Lo cubre incorrectamente
                else if(result == -1)
                    numNeg++;
                
                //Clase por defecto
                else
                {
                    if(defaultClass.equals(ex.getClassAttribute().toString()))
                        numPos++;
                    
                    else
                        numNeg++;
                }
            }
            
            context.write(new Text("positives"), new Text(Long.toString(numPos)));
            context.write(new Text("negatives"), new Text(Long.toString(numNeg)));
            
            //Key: caso (positivos o negativos)
            
            //Value:
            //nº de ejemplos cubiertos
            
        }catch (Exception ex)
        {
            context.write(new Text("Error MAP"), new Text(ex.getMessage() + "\n" + 
                    ex.getLocalizedMessage() + "\n" + ex.toString()));
        }
    }
}
