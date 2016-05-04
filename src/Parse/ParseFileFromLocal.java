/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Parse;

import Hive_JDBC_Operations.DataBase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import Driver_Operations.Driver;
import java.util.Arrays;
import java.util.Set;

/**
 *
 * @author manu
 */
public abstract class ParseFileFromLocal
{
    private static Map<String,Integer>[] inside;//From String to Int, un Map por columna del Dataset
    private static Map<Integer,String>[] outside;//From Int to String, un Map por columna del Dataset
    private static String minorClass;
    
    
    public static void parse(String fileName, String minorClass, int classColNum)
            throws FileNotFoundException, IOException
    {
        File archivo = new File(fileName);
        FileReader fr = new FileReader (archivo);
        BufferedReader br = new BufferedReader(fr);
        String line, cvsSplitBy = ",", insertTmp, insertPositives = "",
                insertNegatives = "";
        String[] lineSplit;
        int numCol, numAtr, limit = Driver.limit, foldPositive = 1,
                foldNegative = 1;
        ParseFileFromLocal.minorClass = minorClass;

        //Leemos una linea del fichero para poder crear las dos tablas
        line = br.readLine();
        lineSplit = line.split(cvsSplitBy);
        numAtr = lineSplit.length;
        numCol = numAtr+1;
        Driver.numAttributes = numAtr;
        
        DataBase.dropTable(Driver.nameBigTablePos);
        DataBase.dropTable(Driver.nameBigTableNeg);
        DataBase.createBigTable(Driver.nameBigTablePos, numCol);
        DataBase.createBigTable(Driver.nameBigTableNeg, numCol);
        
        //Inicializamos las variables privadas
        inside = new Map[numAtr];
        outside = new Map[numAtr];
        
        for (int i = 0; i < numAtr; i++)
        {
            inside[i] = new HashMap();
            outside[i] = new HashMap();
        }
        //--
        
        //Añadimos la primera linea leida al string correspondiente (positivo o
        //negativo
        insertTmp = "(";
        
        for (int i = 0; i < numAtr; i++)
        {
            //Si el valor es nuevo, se añade a las tablas hash para su traducción
            //a int
            if(!inside[i].containsKey(lineSplit[i]))
            {
                inside[i].put(lineSplit[i], inside[i].size()+1);
                outside[i].put(outside[i].size()+1, lineSplit[i]);
            }
            
            insertTmp += inside[i].get(lineSplit[i]) + ",";
        }
        
        if(minorClass.equals(lineSplit[numAtr-1]))
        {
            insertTmp += foldPositive + ")";
            foldPositive++;
            insertPositives += insertTmp;
        }
        else
        {
            insertTmp += foldNegative + ")";
            foldNegative++;
            insertNegatives += insertTmp;
        }
        //--
        
        
        //Introducimos el dataset en el data warehouse
        while((line = br.readLine()) != null)
        {
            lineSplit = line.split(cvsSplitBy);
            insertTmp = "(";

            for (int i = 0; i < numAtr; i++)
            {
                //Si el valor es nuevo, se añade a las tablas hash para su traducción
                if(!inside[i].containsKey(lineSplit[i]))
                {
                    inside[i].put(lineSplit[i], inside[i].size()+1);
                    outside[i].put(outside[i].size()+1, lineSplit[i]);
                }

                insertTmp += inside[i].get(lineSplit[i]) + ",";
            }

            if(minorClass.equals(lineSplit[numAtr-1]))
            {
                insertTmp += foldPositive + ")";
                foldPositive++;
                
                if(foldPositive > 5) foldPositive = 1;
                
                if(!insertPositives.equals("")) insertPositives += "," + insertTmp;
                
                else insertPositives = insertTmp;
                
                if(insertPositives.length() > limit)
                {
                    DataBase.insert(Driver.nameBigTablePos, insertPositives);
                    insertPositives = "";
                }
            }
            else
            {
                insertTmp += foldNegative + ")";
                foldNegative++;
                
                if(foldNegative > 5) foldNegative = 1;
                
                if(!insertNegatives.equals("")) insertNegatives += "," + insertTmp;
                
                else insertNegatives = insertTmp;
                
                if(insertNegatives.length() > limit)
                {
                    DataBase.insert(Driver.nameBigTableNeg, insertNegatives);
                    insertNegatives = "";
                }
            }
            //--
        }
        //--
        
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$-> Final insert...");
        
        //Insertamos los datos restantes
        DataBase.insert(Driver.nameBigTablePos, insertPositives);
        DataBase.insert(Driver.nameBigTableNeg, insertNegatives);
        //--
    }
    
    
    public static String intToString(String line)
    {
        String[] valuesStr = line.split(",");
        String result = "";
        
        for (int i = 0; i < valuesStr.length; i++)
        {
            result += outside[i].get(Integer.parseInt(valuesStr[i])) + ",";
        }
        
        return result;
    }
    
    
    public static String binaryToString(String line)
    {
        String[] valuesStr = line.split(",");
        String result = "";
        
        for (int i = 0; i < valuesStr.length; i++)
        {
            char[] example = valuesStr[i].toCharArray();
            int j = 0;
            int value = example.length;
            
            while(example[j] != '1')
            {
                j++;
                value--;
            }
            
            result += outside[i].get(value) + ",";
        }
        
        return result;
    }
    
    
    public static int[] getNumBits()
    {
        int[] numBits = new int[ParseFileFromLocal.inside.length];
        
        for (int i = 0; i < numBits.length; i++) numBits[i] = ParseFileFromLocal.inside[i].size();
        
        return numBits;
    }
    
    
    public static String getBinaryMinorClass()
    {
        int[] tabNumBits = ParseFileFromLocal.getNumBits();
        int numBits = tabNumBits[tabNumBits.length-1];//El atributo de clase es el último
        
        return ParseFileFromLocal.createBinaryValue(numBits, 
                ParseFileFromLocal.inside[tabNumBits.length-1]
                        .get(ParseFileFromLocal.minorClass));
    }
    
    
    public static String getBinaryMajorityClass()
    {
        int[] tabNumBits = ParseFileFromLocal.getNumBits();
        int size = tabNumBits.length-1;
        int numBits = tabNumBits[size];//El atributo de clase es el último
        Set<String> s = ParseFileFromLocal.inside[size].keySet();
        s.remove(ParseFileFromLocal.minorClass);
        String majorityClass = s.iterator().next();
        
        return ParseFileFromLocal.createBinaryValue(numBits, 
                ParseFileFromLocal.inside[size].get(majorityClass));
    }
    
    
    public static String createBinaryValue(int numBits, int posiTrue)
    {
        String value = "";
        
        for (int i = 1; i <= numBits; i++)
        {
            if(posiTrue == i) value = "1" + value;
            
            else value = "0" + value;
        }
        
        return value;
    }
}
