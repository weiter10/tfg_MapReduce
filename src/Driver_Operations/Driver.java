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
import MapReduce.Map1;
import MapReduce.Reduce1;
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
            nameTestTableTest = "testset",
            nameRuleTable = "tabrules";
    
    public static int numAttributes, limit = Integer.MAX_VALUE/4;
    
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
        for (int i = 1; i <= 5; i++)
        {
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = "/output";
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Generamos el fichero a través del cual se lanzarán los Map1
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing Dataset in HDFS");
            Driver.generateTrainingSetFile(i);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Dataset write in HDFS OK");
            
            //Lanzamos la tarea MR con los algoritmos genéticos
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Job1_" + i);
            ToolRunner.run(new Job1(), args);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Job1_" + i + " OK");
            
            //Escribimos el resultado en el almacenamiento local
            args2[0] = "/output/part-r-00000";
            args2[1] = args[3] + "/Job1_" + i;
            ToolRunner.run(new HdfsReaderExt(), args2);
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            args2[0] = "/input/dataset";
            args2[1] =  args[3] + "/datasetHDFS_" + i;
            ToolRunner.run(new HdfsReaderExt(), args2);
            
            //Introducimos las reglas en Hive
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing rules in Hive");
            DataBase.createRuleTable(Driver.nameRuleTable);
            DataBase.load(Driver.nameRuleTable,"/output/part-r-00000");
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Rules write in Hive OK");
            
            //Creamos el fichero de entrada con las reglas y el testset
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing testSet in HDFS");
            Driver.generateTestSetFile(i);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> TestSet write in HDFS OK");
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            args2[0] = "/input/testset";
            args2[1] = args[3] + "/testsetHDFS_" + i;
            ToolRunner.run(new HdfsReaderExt(), args2);
            
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = "/output";
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Lanzamos el MR que determinará la precisión del clasificador
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Job2_" + i);
            ToolRunner.run(new Job2(), args);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Job2_" + i + " OK");
            
            //Escribimos el resultado en el almacenamiento local
            args2[0] = "/output/part-r-00000";
            args2[1] = args[3] + "/Job2_" + i;
            ToolRunner.run(new HdfsReaderExt(), args2);
        }
        
        System.exit(0);
    }
    
    
    public static void generateTestSetFile(int testFold) throws Exception
    {
        int[] numBits = ParseFileFromLocal.getNumBits();
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/testset"});
        BufferedWriter br = HdfsWriter.br;
        
        //Escribimos las reglas en el fichero
        DataBase.writeTestSet(Driver.nameBigTablePos, Driver.nameBigTableNeg,
                Driver.nameRuleTable, testFold, numBits, br);
        
        br.close();
    }

    
    public static float calculateIR()
    {
        long numNeg = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        long numPos = DataBase.getNumRows(Driver.nameTableTrainingPos);
        
        return (float)numNeg/numPos;
    }
    
    
    public static void generateTrainingSetFile(int testFold) throws Exception
    {
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
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/dataset"});
        BufferedWriter br = HdfsWriter.br;
        
        //El IR del dataset que recibe el genético va a ser 1
        int irAGL = 1;
        iR = Driver.calculateIR();
        
        for (int i = 0; i < (int)iR; i++)
        {
            value = (i*positiveSize)+1;
            //Escribimos el IR
            br.write(irAGL + "\t");
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, br);
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, value+positiveSize-1, br);
            br.write("\n");
        }
        
        //Comprobamos si existe un último split que no tenía suficientes ejemplos
        //en la clase negativa (mayoritaria)
        if((negativeSize%positiveSize) != 0)
        {
            value = (((int)iR)*positiveSize)+1;
            sizeSubsection = positiveSize - (negativeSize - (((int)iR) * positiveSize));
            //Obtenemos el spilt final
            //Escribimos el IR
            br.write(irAGL + "\t");
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, br);
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, negativeSize, br);
            //Le añadimos el trozo faltante con ejemplos del primer split
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, 1, sizeSubsection, br);
            br.write("\n");
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
