/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Driver_Operations;


import Hdfs_Operations.HdfsReaderExt;
import Hdfs_Operations.HdfsRemove;
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
import java.util.Arrays;
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
public class Driver
{
    public static String nameBigTablePos = "positiveexamples",
            nameBigTableNeg = "negativeexamples",
            nameTableTrainingPos = "positiveexamplestraining",
            nameTableTrainingNeg = "negativeexamplestraining",
            nameTestTableTest = "testset";
    
    public static int numAttributes;
    
    public static void main(String[] args) throws Exception
    {
        String[] args2 = new String[2];
        
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
        
        //Generamos 5 cross validation
        for (int i = 1; i <= 1; i++)
        {
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = "/output";
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Generamos el fichero a través del cual se lanzarán los Map
            Driver.generateBigFile(i);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Dataset write in HDFS OK");


            //Lanzamos la tarea
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting job " + i);
            ToolRunner.run(new JobAlgorithm(), args);
            
            //args2[0] = "/output/part-r-00000";
            args2[0] = "/output/part-r-00000";
            args2[1] = args[3] + i;

            //Escribimos el resultado en el almacenamiento local
            ToolRunner.run(new HdfsReaderExt(), args2);

            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            args2[0] = "/input/dataset";
            args2[1] = "/home/manu/datasetHDFS" + i;
            ToolRunner.run(new HdfsReaderExt(), args2);
        }
        
        System.exit(0);
        
    }

    
    public static float calculateIR()
    {
        long numNeg = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        long numPos = DataBase.getNumRows(Driver.nameTableTrainingPos);
        
        return (float)numNeg/numPos;
    }
    
    
    public static void generateBigFile(int testFold) throws Exception
    {
        String positiveExamples, line;
        long positiveSize, negativeSize;
        long value, sizeSubsection;
        int[] numBits, trainingFolds = new int[4];
        float iR;
        
        //Escogemos los folds para training
        for (int i=1, j=0; i <= 5; i++, j++)
        {
            if(i != testFold)
                trainingFolds[j] = i;

            else j--;
        }
        
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Training folds " + Arrays.toString(trainingFolds));
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Test fold: " + testFold);
        
        //Construimos las tablas
        DataBase.createTrainingTable(Driver.nameBigTablePos, Driver.nameTableTrainingPos, trainingFolds);
        DataBase.createTrainingTable(Driver.nameBigTableNeg, Driver.nameTableTrainingNeg, trainingFolds);
        
        numBits = ParseFileFromLocal.getNumBits();
        positiveSize = DataBase.getNumRows(Driver.nameTableTrainingPos);
        negativeSize = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        positiveExamples = DataBase.getDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize);
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/dataset"});
        BufferedWriter br = HdfsWriter.br;
        
        //El IR del dataset que recibe el genético va a ser 1
        int irAGL = 1;
        iR = Driver.calculateIR();
        
        for (int i = 0; i < (int)iR; i++)
        {
            value = (i*positiveSize)+1;
            line = irAGL + "\t" + positiveExamples + DataBase.getDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, value+positiveSize-1) + "\n";
            br.write(line);//Escribimos en HDFS
        }
        
        //Comprobamos si existe un último split que no tenía suficientes ejemplos
        //en la clase negativa (mayoritaria)
        if((negativeSize%positiveSize) != 0)
        {
            value = (((int)iR)*positiveSize)+1;
            sizeSubsection = positiveSize - (negativeSize - (((int)iR) * positiveSize));
            //Obtenemos el spilt final
            line = irAGL + "\t" + positiveExamples + DataBase.getDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, negativeSize);
            //le añadimos el trozo faltante con ejemplos del primer split
            line += DataBase.getDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, 1, sizeSubsection) + "\n";
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
