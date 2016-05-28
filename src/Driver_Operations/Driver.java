/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Driver_Operations;


import AGL.Algorithm;
import Hdfs_Operations.HdfsReader;
import Job_Test.Test;
import Job_Training.Training;
import Hdfs_Operations.HdfsReaderToLocal;
import Hdfs_Operations.HdfsRemove;
import Hdfs_Operations.HdfsWriter;
import Hive_JDBC_Operations.DataBase;
import Job_GlobalEvaluation.GlobalEvaluation;
import Parse.ParseFileFromLocal;
import Job_Training.MapTraining;
import Job_Training.ReduceTraining;
import Local_Storage_Operations.LocalStorageWrite;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.time.LocalDateTime;


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
            nameOrderedRuleTable = "taborderedrules",
            nameBulkRuleTable = "tabbulkrules",
            nameGlobalEvaluationFile = "globalEvaluation",
            nameTrainingSetFile = "trainingSet";
    
    public static long sizeTrainingSetFile, sizeGlobalEvaluationFile, numMaps;
    
    public static int numAttributes, limit = Integer.MAX_VALUE/4;
    
    private static int countSeedRnd = 0;
    
    public static void main(String[] args) throws Exception
    {
        String[] args2 = new String[2];
        String nameFileOutputMR = "/output/part-r-00000", str;
        int numFolds = 5;//MAX 5
        double[] accuracy = new double[numFolds];
        double meanAccuracy = 0;
        
        for (int i = 0; i < numFolds; i++)
            accuracy[i] = 0;
        
        
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
        for (int i = 1; i <= numFolds; i++)
        {
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = "/output";
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Generamos el fichero a través del cual se lanzarán los AGL
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing Dataset in HDFS");
            Driver.generateTrainingSetFile(Driver.nameTrainingSetFile, i);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Dataset write in HDFS OK");
            
            //Lanzamos la tarea MR con los algoritmos genéticos
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Training_" + i);
            ToolRunner.run(new Training(), args);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Training_" + i + " OK");
            
            //Escribimos el resultado en el almacenamiento local
            args2[0] = nameFileOutputMR;
            args2[1] = args[3] + "/Training_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            args2[0] = "/input/" + Driver.nameTrainingSetFile;
            args2[1] =  args[3] + "/" + Driver.nameTrainingSetFile + "_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Creamos el fichero de entrada para la evaluación global
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing globalEvaluation file in HDFS");
            Driver.generateGlobalEvatuationFile(Driver.nameTrainingSetFile, 
                    Driver.nameGlobalEvaluationFile, nameFileOutputMR);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> GlobalEvaluation file in HDFS OK");
            
            //Escribimos el fichero de entrada de la evaluación global en el
            //almacenamiento local
            args2[0] = "/input/" + Driver.nameGlobalEvaluationFile;
            args2[1] = args[3] + "/GlobalEvaluationInput_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
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
            args2[1] = args[3] + "/GlobalEvaluation_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Creamos el fichero de entrada con las reglas y el testset
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing testSet in HDFS");
            Driver.generateTestSetFile(i, nameFileOutputMR);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> TestSet write in HDFS OK");
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            args2[0] = "/input/testset";
            args2[1] = args[3] + "/testsetHDFS_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = "/output";
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Lanzamos el MR que determinará la precisión del clasificador
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Test_" + i);
            ToolRunner.run(new Test(), args);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Test_" + i + " OK");
            
            //Escribimos el resultado en el almacenamiento local
            args2[0] = nameFileOutputMR;
            args2[1] = args[3] + "/Test_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Obtenemos la precisión del clasificador
            accuracy[i-1] = Driver.getAccuracy(nameFileOutputMR);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> AccuracyFold_" + i + ": "
                    + "accuracy[i-1]");
        }
        
        //
        str = args[0] + "\n";
        
        for (int i = 0; i < numFolds; i++)
        {
            str += "Precision del clasificador " + (i+1) + ": " + accuracy[i] + "\n";
            meanAccuracy += accuracy[i];
        }
        
        str += "\nPrecision media del clasificador: " + meanAccuracy/numFolds;
        str += "\n\nTamanio poblacion: " + Algorithm.sizePopulation + 
                "\nNumero de iteraciones sin mejora: " + Algorithm.limit;
        LocalStorageWrite.run(args[3] + "/Accuracy_" + LocalDateTime.now() + 
                ".txt", str);
        
        System.exit(0);
    }
    
    
    private static double getAccuracy(String fileName) throws Exception
    {
        String line;
        String[] data;
        double true_positives = 0, true_negatives = 0, false_positives = 0,
                false_negatives = 0, resMinor, resMajor;
        
        //Obtenemos el buffer de lectura en HDFS
        ToolRunner.run(new HdfsReader(), new String[] {fileName});
        BufferedReader br = HdfsReader.br;
        
        while((line = br.readLine()) != null)
        {
            data = line.split("\t");
            
            switch(data[0])
            {
                case "true_positives":
                    true_positives = Long.parseLong(data[1]);
                    break;
                    
                case "true_negatives":
                    true_negatives = Long.parseLong(data[1]);
                    break;
                    
                case "false_positives":
                    false_positives = Long.parseLong(data[1]);
                    break;
                    
                case "false_negatives":
                    false_negatives = Long.parseLong(data[1]);
                    break;
            }
        }
        
        resMinor = true_positives/(true_positives + false_negatives);
        resMajor = true_negatives/(true_negatives + false_positives);
        
        return Math.sqrt(resMinor * resMajor);
    }
    
    
    private static void generateGlobalEvatuationFile(String trainingSetFileName, 
            String fileName, String fileRulesName) throws Exception
    {
        String line;
        
        //Introducimos las reglas en Hive
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing bulk rules in Hive");
        DataBase.createRuleTable(Driver.nameBulkRuleTable);
        DataBase.load(Driver.nameBulkRuleTable, fileRulesName);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Bulk rules write in Hive OK");
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/" + fileName});
        BufferedWriter bw = HdfsWriter.bw;
        
        //Obtenemos el buffer de lectura en HDFS
        ToolRunner.run(new HdfsReader(), new String[] {"/input/" + trainingSetFileName});
        BufferedReader br = HdfsReader.br;
        
        //Escribimos las reglas en el fichero
        while((line = br.readLine()) != null)
        {
            //Escribimos las reglas
            DataBase.writeBulkRulesInFile(Driver.nameBulkRuleTable, bw);
            //Escribimos los datos(ejemplos de entrenamiento)
            bw.write("\t" + line + "\n");
        }
        
        bw.close();
        br.close();
        
        Driver.sizeGlobalEvaluationFile = HdfsWriter.fs.getFileStatus(new Path("/input/" + fileName)).getLen();
    }
    
    
    private static void generateTestSetFile(int testFold, String fileRulesName) throws Exception
    {
        int[] numBits = ParseFileFromLocal.getNumBits();
        
        //Introducimos las reglas en Hive
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing ordered rules in Hive");
        DataBase.createRuleTable(Driver.nameOrderedRuleTable);
        DataBase.load(Driver.nameOrderedRuleTable, fileRulesName);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Ordered rules write in Hive OK");
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/testset"});
        BufferedWriter bw = HdfsWriter.bw;
        
        //Escribimos las reglas en el fichero
        DataBase.writeTestSet(Driver.nameBigTablePos, Driver.nameBigTableNeg,
                Driver.nameOrderedRuleTable, testFold, numBits, bw);
        
        bw.close();
    }

    
    private static float calculateIR()
    {
        long numNeg = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        long numPos = DataBase.getNumRows(Driver.nameTableTrainingPos);
        
        return (float)numNeg/numPos;
    }
    
    
    private static void generateTrainingSetFile(String fileName, int testFold) throws Exception
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
        
        //Construimos las tablas de Training
        DataBase.createTrainingTable(Driver.nameBigTablePos, Driver.nameTableTrainingPos, trainingFolds);
        DataBase.createTrainingTable(Driver.nameBigTableNeg, Driver.nameTableTrainingNeg, trainingFolds);
        
        numBits = ParseFileFromLocal.getNumBits();
        positiveSize = DataBase.getNumRows(Driver.nameTableTrainingPos);
        negativeSize = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/" + fileName});
        BufferedWriter bw = HdfsWriter.bw;
        
        //El IR del dataset que recibe el genético va a ser 1, ya que las clases
        //están totalmente balanceadas
        int irAGL = 1;
        iR = Driver.calculateIR();
        Driver.numMaps = (long)iR;
        
        for (int i = 0; i < (int)iR; i++)
        {
            value = (i*positiveSize)+1;
            //Escribimos la semilla aleatoria y el IR
            bw.write(Driver.countSeedRnd + "\t\t" + irAGL + "\t\t");
            Driver.countSeedRnd++;
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, bw);
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, value+positiveSize-1, bw);
            bw.write("\n");
        }
        
        //Comprobamos si existe un último split que no tenía suficientes ejemplos
        //en la clase negativa (mayoritaria)
        if((negativeSize%positiveSize) != 0)
        {
            value = (((int)iR)*positiveSize)+1;
            sizeSubsection = positiveSize - (negativeSize - (((int)iR) * positiveSize));
            //Obtenemos el spilt final
            //Escribimos la semilla aleatoria y el IR
            bw.write(Driver.countSeedRnd + "\t\t" + irAGL + "\t\t");
            Driver.countSeedRnd++;
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, bw);
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, negativeSize, bw);
            //Le añadimos el trozo faltante con ejemplos del primer split
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, 1, sizeSubsection, bw);
            bw.write("\n");
        }
        
        bw.close();//Cerramos el buffer de escritura HDFS
        Driver.sizeTrainingSetFile = HdfsWriter.fs.getFileStatus(new Path("/input/" + fileName)).getLen();
        
        
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
