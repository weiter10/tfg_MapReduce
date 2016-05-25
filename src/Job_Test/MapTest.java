/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Test;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
public class MapTest extends Mapper<LongWritable, Text, Text, Text>
{

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws InterruptedException, IOException
    {
        try
        {
            int result, index;
            Map<String, Integer> indexConfusionMatrix = new HashMap();
            long[][] confusionMatrix = new long[2][2];
            String[] data = value.toString().split("\t\t");
            String minorClass = data[0], majorityClass = data[1], 
                    defaultClass = data[1], exClass;
            //Cargamos las reglas
            ArrayList<Rule> rules = Parse.parseRules(data[2]);
            Example ex;
            
            //Agregamos la clase minoritaria y mayoritaria al diccionario de 
            //índices
            indexConfusionMatrix.put(data[0], 0);
            indexConfusionMatrix.put(data[1], 1);
            
            /*
            Fila: dada por el clasificador (predicha)
            Columna: dada por el ejemplo(real)
            */
            
            for(String exampleStr : data[3].split("\t"))
            {
                result = 0;
                index = 0;
                ex = Parse.parseExample(exampleStr);
                exClass = ex.getClassAttribute().toString();
                
                while(index < rules.size() && result == 0)
                {
                    result = rules.get(index).coverExample(ex);
                    
                    if(result == 0)
                        index++;
                }
                
                //Lo cubre de manera correcta o incorrecta
                if(result == 1 || result == -1)
                    confusionMatrix[indexConfusionMatrix.get(rules.get(index).getStrClass())]
                            [indexConfusionMatrix.get(ex.getClassAttribute().toString())]++;
                
                //Clase por defecto (mayoritaria)
                else
                    confusionMatrix[indexConfusionMatrix.get(defaultClass)]
                        [indexConfusionMatrix.get(ex.getClassAttribute().toString())]++;
            }
            
            context.write(new Text("true_positives"), new Text(Long.toString(confusionMatrix[0][0])));
            context.write(new Text("true_negatives"), new Text(Long.toString(confusionMatrix[1][1])));
            context.write(new Text("false_positives"), new Text(Long.toString(confusionMatrix[0][1])));
            context.write(new Text("false_negatives"), new Text(Long.toString(confusionMatrix[1][0])));
            
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
