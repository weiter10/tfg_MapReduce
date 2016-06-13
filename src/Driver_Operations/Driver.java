/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Driver_Operations;

import Hdfs_Operations.HdfsCreateDirectory;
import Hdfs_Operations.HdfsReader;
import Job_Test.Test;
import Job_Training.Training;
import Hdfs_Operations.HdfsReaderToLocal;
import Hdfs_Operations.HdfsRemove;
import Hdfs_Operations.HdfsWriter;
import Hive_JDBC_Operations.DataBase;
import Job_GlobalEvaluation.GlobalEvaluation;
import Job_Training.InfoRule;
import Local_Storage_Operations.LocalStorageCreateFolder;
import Parse.ParseFileFromLocal;
import Local_Storage_Operations.LocalStorageWrite;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
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
            nameOrderedRulesFile = "orderedRules",
            nameTrainingSetFile = "trainingSet",
            pathFolderTraining = "/input/trainingFiles",
            pathFolderTestFile = "/input/testFiles",
            pathFolderOutputMR = "/output";
    
    public static int numAttributes, maxSizeStr = Integer.MAX_VALUE/4, 
            algorithmIter, algorithmSizeP;
    
    public static long countSeedRnd;
    
    public static void main(String[] args) throws Exception
    {
        String[] args2 = new String[2];
        String nameFileOutputMR = pathFolderOutputMR + "/part-r-00000", str,
                outputFolder;
        int numFolds = 5;//MAX 5
        long[] numRules = new long[numFolds];
        double[] accuracy = new double[numFolds];
        double meanAccuracy = 0;
        long initTime, finishTime;
        
        for (int i = 0; i < numFolds; i++)
            accuracy[i] = 0;
        
        if(args.length < 10)
        {
            System.err.println("Number of arguments incorrect");
            System.err.println("Local_dataset Name_positive_class Num_colum_positive_class"
                    + " Separator File_output_name Balanced(true/false) SizePopullaion "
                    + "numIterationsWithOutImprove GlobalEvaluation(true/false) "
                    + "Seed");
            System.exit(1);
        }
        
        //Inicializamos las variables
        algorithmSizeP = Integer.parseInt(args[6]);
        algorithmIter = Integer.parseInt(args[7]);
        
        //Inicializamos la semilla
        countSeedRnd = Long.parseLong(args[9]);
        
        //Construimos el directorio de salida
        outputFolder = args[4] + "/" + Long.toString(System.currentTimeMillis());
        LocalStorageCreateFolder.run(outputFolder);
        
        initTime = System.currentTimeMillis();
        
        //Creamos el data warehouse a partir del dataset de entrada
        ParseFileFromLocal.parse(args[0], args[1], Integer.parseInt(args[2]), args[3]);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Data warehouse OK");
        //--
        
        //Generamos 5 cross validation
        for (int i = 1; i <= numFolds; i++)
        {
            //Borramos el directorio de salida de trabajos MapReduce
            args2[0] = pathFolderOutputMR;
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            
            //Borramos el directorio de los training set con todo el contenido
            //y lo volvemos a crear
            args2[0] = pathFolderTraining;
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            ToolRunner.run(new HdfsCreateDirectory(), args2);
            
            //Borramos el directorio de los testset con todo el contenido
            //y lo volvemos a crear
            args2[0] = pathFolderTestFile;
            args2[1] = "";
            ToolRunner.run(new HdfsRemove(), args2);
            ToolRunner.run(new HdfsCreateDirectory(), args2);
            
            //Generamos los ficheros a través de los cuales se lanzarán los AGL
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing Dataset in HDFS");
            
            if(args[5].equals("true"))
                Driver.generateTrainingSetFileB(i);
            
            else
                Driver.generateTrainingSetFileNB(i);
            
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Dataset write in HDFS OK");
            
            //Lanzamos la tarea MR con los algoritmos genéticos
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting Training_" + i);
            ToolRunner.run(new Training(), args);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Training_" + i + " OK");
            
            //Escribimos el resultado en el almacenamiento local
            args2[0] = nameFileOutputMR;
            args2[1] = outputFolder + "/Training_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            //args2[0] = "/input/" + Driver.nameTrainingSetFile;
            //args2[1] =  args[4] + "/" + Driver.nameTrainingSetFile + "_" + i;
            //ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Evaluacion global
            if(args[8].equals("true"))
            {
                //Creamos el fichero de entrada para la evaluación global
                System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing globalEvaluation file in HDFS");
                Driver.generateGlobalEvatuationFile(nameFileOutputMR);
                System.out.println("$$$$$$$$$$$$$$$$$$$$$-> GlobalEvaluation file in HDFS OK");

                
                //Escribimos el fichero de entrada de la evaluación global en el
                //almacenamiento local
                //args2[0] = "/input/" + Driver.nameGlobalEvaluationFile;
                //args2[1] = outputFolder + "/GlobalEvaluationInput_" + i;
                //ToolRunner.run(new HdfsReaderToLocal(), args2);

                //Borramos el directorio de salida de trabajos MapReduce
                args2[0] = pathFolderOutputMR;
                args2[1] = "";
                ToolRunner.run(new HdfsRemove(), args2);

                //Lanzamos la evaluación global
                System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Starting GlobalEvaluation_" + i);
                ToolRunner.run(new GlobalEvaluation(), args);
                System.out.println("$$$$$$$$$$$$$$$$$$$$$-> GlobalEvaluation_" + i + " OK");

                //Escribimos el resultado en el almacenamiento local
                args2[0] = nameFileOutputMR;
                args2[1] = outputFolder + "/GlobalEvaluationOutput_" + i;
                ToolRunner.run(new HdfsReaderToLocal(), args2);
            }
            
            //Creamos el fichero con las reglas ordenadas por PI
            //Y una serie de ficheros con los datos de test que serán el directorio que
            //contiene a estos ficheros será en de entrada del MAP
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing testSet in HDFS");
            Driver.generateTestSetFile(i, nameFileOutputMR);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> TestSet write in HDFS OK");
            
            //Almacenamos el numero de reglas para el reporte final
            numRules[i-1] = DataBase.getNumRows(Driver.nameOrderedRuleTable);
            
            //Escritura del fichero de entrada de los MAP en el almacenamiento local
            //args2[0] = "/input/testset";
            //args2[1] = args[4] + "/testsetHDFS_" + i;
            //ToolRunner.run(new HdfsReaderToLocal(), args2);
            
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
            args2[1] = outputFolder + "/Test_" + i;
            ToolRunner.run(new HdfsReaderToLocal(), args2);
            
            //Obtenemos la precisión del clasificador
            accuracy[i-1] = Driver.getAccuracy(nameFileOutputMR);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$-> AccuracyFold_" + i + ": "
                    + accuracy[i-1]);
        }
        
        finishTime = System.currentTimeMillis() - initTime;
        
        long totalSecs = (long) (finishTime/1000F);
        int days = (int) (totalSecs / 86400);
        int hours = (int) ((totalSecs % 86400)/3600);
        int minutes = (int) ((totalSecs % 3600) / 60);
        int seconds = (int) (totalSecs % 60);

        String timeString = String.format("%02d:%02d:%02d:%02d", days, hours, minutes, seconds);
        
        //
        str = args[0] + "\n\n";
        
        for (int i = 0; i < numFolds; i++)
        {
            str += "Precision del clasificador " + (i+1) + ": " + String.format("%.2f", accuracy[i]*100) +
                    "\tNumero de reglas: " + numRules[i] +"\n";
            meanAccuracy += accuracy[i]*100;
        }
        
        str += "\nPrecision media del clasificador: " + String.format("%.2f",(meanAccuracy/numFolds))
                + "\nIR: " + String.format("%.2f", Driver.calculateIRGlobal()) + "\n\nTamanio poblacion: "
                + Driver.algorithmSizeP + "\nNumero de iteraciones sin mejora: "
                + Driver.algorithmIter + "\nEvaluacion Global: " + args[8]
                + "\nPonderado: " + InfoRule.weighted + "\nAplicación de balanceado de clases: "
                + (!Boolean.parseBoolean(args[5]))
                + "\nTiempo de ejecucion (Dias:Horas:Minutos:Segundos) => " + timeString
                + "\nSemilla aleatoria inicial: " + args[9];
        
        String[] tab = args[0].split("/");
        
        LocalStorageWrite.run(outputFolder + "/Accuracy_" + tab[tab.length-1] + "_"
                + LocalDateTime.now() + ".txt", str);
        
        System.exit(0);
    }
    
    /**
     * Obtiene la precisión del clasificador
     * @param fileName
     * @return
     * @throws Exception 
     */
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
    
    /**
     * Genera el fichero de evaluación global
     * @param trainingSetFileName
     * @param fileName
     * @param fileRulesName
     * @throws Exception 
     */
    private static void generateGlobalEvatuationFile(String fileRulesName) throws Exception
    {
        //Introducimos las reglas en Hive
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing bulk rules in Hive");
        DataBase.createRuleTable(Driver.nameBulkRuleTable);
        DataBase.load(Driver.nameBulkRuleTable, fileRulesName);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Bulk rules write in Hive OK");
        
        //Obtenemos el buffer de escritura en HDFS
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/" + Driver.nameGlobalEvaluationFile});
        BufferedWriter bw = HdfsWriter.bw;
        
        //Escribimos las reglas
        DataBase.writeBulkRulesInFile(Driver.nameBulkRuleTable, bw);
        
        bw.close();
    }
    
    
    private static void generateTestSetFile(int testFold, String fileRulesName) throws Exception
    {
        int[] numBits = ParseFileFromLocal.getNumBits();
        
        //Introducimos las reglas en Hive
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Writing ordered rules in Hive");
        DataBase.createRuleTable(Driver.nameOrderedRuleTable);
        DataBase.load(Driver.nameOrderedRuleTable, fileRulesName);
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Ordered rules write in Hive OK");
        
        //Escribimos las reglas ordenadas por PI en un fichero
        ToolRunner.run(new HdfsWriter(), new String[] {"/input/" + nameOrderedRulesFile});
        BufferedWriter bwRules = HdfsWriter.bw;
        DataBase.writeOrderedRulesInFile(nameOrderedRuleTable, bwRules);
        bwRules.close();
        
        //Se generan los ficheros de test
        DataBase.writeTestSet(Driver.nameBigTablePos, Driver.nameBigTableNeg,
                testFold, numBits);
    }

    
    private static float calculateIRGlobal()
    {
        long numNeg = DataBase.getNumRows(Driver.nameBigTableNeg);
        long numPos = DataBase.getNumRows(Driver.nameBigTablePos);
        
        return (float)numNeg/numPos;
    }
    
    private static float calculateIRFold()
    {
        long numNeg = DataBase.getNumRows(Driver.nameTableTrainingNeg);
        long numPos = DataBase.getNumRows(Driver.nameTableTrainingPos);
        
        return (float)numNeg/numPos;
    }
    
    /**
     * Genera el traning set para clases no balanceadas
     * @param testFold
     * @throws Exception 
     */
    private static void generateTrainingSetFileNB(int testFold) throws Exception
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
        
        //El IR del dataset que recibe el genético va a ser 1, ya que las clases
        //están totalmente balanceadas
        int irAGL = 1;
        iR = Driver.calculateIRFold();
        
        for (int i = 0; i < (int)iR; i++)
        {
            //Obtenemos el buffer de escritura en HDFS
            ToolRunner.run(new HdfsWriter(), new String[] {pathFolderTraining + 
                    "/" + nameTrainingSetFile + i});
            BufferedWriter bw = HdfsWriter.bw;
            
            value = (i*positiveSize)+1;
            
            //Escribimos la semilla aleatoria y el IR
            bw.write(Driver.countSeedRnd + "\t\t" + irAGL + "\t\t" +
                    Driver.algorithmSizeP + "\t\t" + Driver.algorithmIter + "\t\t");
            Driver.countSeedRnd++;
            
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, bw);
            
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, value+positiveSize-1, bw);
            bw.write("\n");
            
            bw.close();//Cerramos el buffer de escritura HDFS
        }
        
        //Comprobamos si existe un último split que no tenía suficientes ejemplos
        //en la clase negativa (mayoritaria)
        if((negativeSize%positiveSize) != 0)
        {
            //Obtenemos el buffer de escritura en HDFS
            ToolRunner.run(new HdfsWriter(), new String[] {pathFolderTraining + 
                    "/" + nameTrainingSetFile + (((int)iR)+1)});
            BufferedWriter bw = HdfsWriter.bw;
            
            value = (((int)iR)*positiveSize)+1;
            sizeSubsection = positiveSize - (negativeSize - (((int)iR) * positiveSize));
            
            //Obtenemos el spilt final
            //Escribimos la semilla aleatoria y el IR
            bw.write(Driver.countSeedRnd + "\t\t" + irAGL + "\t\t" +
                    Driver.algorithmSizeP + "\t\t" + Driver.algorithmIter + "\t\t");
            Driver.countSeedRnd++;
            
            //Escribimos los ejemplos positivos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingPos, numBits, 1, positiveSize, bw);
            
            //Escribimos los ejemplos negativos
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, value, negativeSize, bw);
            
            //Le añadimos el trozo faltante con ejemplos del primer split
            DataBase.writeDataBinaryFormat(Driver.nameTableTrainingNeg, numBits, 1, sizeSubsection, bw);
            bw.write("\n");
            bw.close();//Cerramos el buffer de escritura HDFS
        }
        
        
        /*
        System.out.println("-------------------------------------------------------------");
        Driver.showDataBinaryFormat(positiveExamples);
        System.out.println("-------------------------------------------------------------");
        */
    }
    
    
    /**
     * Genera los training set para clases balanceadas
     * @param testFold
     * @throws Exception 
     */
    private static void generateTrainingSetFileB(int testFold) throws Exception
    {
        int[] numBits, trainingFolds = new int[4];
        
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
        
        DataBase.writeBalancedDataBinaryFormat(nameTableTrainingPos, nameTableTrainingNeg
                , pathFolderTraining + "/" + nameTrainingSetFile, numBits);
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
