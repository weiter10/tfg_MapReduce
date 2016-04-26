/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Driver_Operations;


import Hdfs_Operations.HdfsReaderExt;
import Hdfs_Operations.HdfsWriter;
import Hive_JDBC_Operations.DataBase;
import Parse.ParseFileFromLocal;
import MapReduce.Map;
import MapReduce.Reduce;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author manu
 */
//Entrada primitiva: "/input/inputMR" "/output"
public class Driver extends Configured implements Tool
{
    public static String nameTablePos = "positiveexamples",
            nameTableNeg = "negativeexamples";
    
    
    public static void main(String[] args) throws Exception
    {
        if(args.length < 4)
        {
            System.err.println("Number of arguments incorrect");
            System.err.println("Local_dataset_name Name_positive_class Num_colum_positive_class"
                    + "File_output_name");
            System.exit(1);
        }
        
        
        //Introducimos el fichero en el data warehouse
        ParseFileFromLocal.parse(args[0], args[1], Integer.parseInt(args[2]));
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Data warehouse OK");
        //--
        
        //Generamos el fichero a través del cual se lanzarán los Map
        Driver.generateBigFile();
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Dataset write in HDFS OK");
        
        
        //Lanzamos la tarea
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting");
        ToolRunner.run(new Driver(), args);
        
        String[] args2 = new String[2];
        //args2[0] = "/output/part-r-00000";
        args2[0] = "/output/part-r-00000";
        args2[1] = args[3];
        
        //Escribimos el resultado en el almacenamiento local
        ToolRunner.run(new HdfsReaderExt(), args2);
        /*
        //Escritura del fichero de entrada de los MAP en el almacenamiento local
        args2[0] = "/input/dataset";
        args2[1] = "/home/manu/smallHDFS";
        ToolRunner.run(new HdfsReaderExt(), args2);
        */
        System.exit(0);
        
    }

    @Override
    public int run(String[] args) throws Exception
    {
        Configuration conf = getConf();
        
        Job job = new Job(conf, "DGA");
        
        //Establecer un número de reduces concretos
        //job.setNumReduceTasks(1);
        
        job.setJarByClass(Driver.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("/input/dataset"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        
        job.waitForCompletion(true);
        
        return 0;
    }
    
    
    public static float calculateIR()
    {
        long numNeg = DataBase.getNumRows(Driver.nameTableNeg);
        long numPos = DataBase.getNumRows(Driver.nameTablePos);
        
        return (float)numNeg/numPos;
    }
    
    
    public static void generateBigFile() throws Exception
    {
        String positiveExamples, line;
        float iR;
        long positiveSize, negativeSize;
        long value, sizeSubsection;
        int[] numBits;
        
        numBits = ParseFileFromLocal.getNumBits();
        iR = Driver.calculateIR();
        positiveSize = DataBase.getNumRows(Driver.nameTablePos);
        negativeSize = DataBase.getNumRows(Driver.nameTableNeg);
        positiveExamples = DataBase.getDataBinaryFormat(Driver.nameTablePos, numBits, 1, positiveSize);
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/dataset"});
        BufferedWriter br = HdfsWriter.br;
        
        for (int i = 0; i < (int)iR; i++)
        {
            value = (i*positiveSize)+1;
            line = positiveExamples + DataBase.getDataBinaryFormat(Driver.nameTableNeg, numBits, value, value+positiveSize-1) + "\n";
            br.write(line);//Escribimos en HDFS
        }
        
        //Comprobamos si existe un último split que no tenía suficientes ejemplos
        //en la clase negativa (mayoritaria)
        if((negativeSize%positiveSize) != 0)
        {
            value = (((int)iR)*positiveSize)+1;
            sizeSubsection = positiveSize - (negativeSize - (((int)iR) * positiveSize));
            //Obtenemos el spilt final
            line = positiveExamples + DataBase.getDataBinaryFormat(Driver.nameTableNeg, numBits, value, negativeSize);
            //le añadimos el trozo faltante con ejemplos del primer split
            line += DataBase.getDataBinaryFormat(Driver.nameTableNeg, numBits, 1, sizeSubsection) + "\n";
            br.write(line);//Escribimos en HDFS
        }
        
        br.close();//Cerramos el buffer de escritura HDFS
        /*
        System.out.println("-------------------------------------------------------------");
        Driver.showDataBinaryFormat(positiveExamples);
        System.out.println("-------------------------------------------------------------");
        */
    }
    
    
    private static void showDataIntegerFormat(String data)
    {
        String[] lines = data.split("\t");
        
        for (int i = 0; i < lines.length; i++)
        {
            System.out.println(ParseFileFromLocal.intToString(lines[i]));
        }
    }
    
    
    private static void showDataBinaryFormat(String data)
    {
        String[] lines = data.split("\t");
        
        for (int i = 0; i < lines.length; i++)
        {
            System.out.println(ParseFileFromLocal.binaryToString(lines[i]));
        }
    }
}
