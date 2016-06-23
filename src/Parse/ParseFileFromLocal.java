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
import Main_Operations.Main;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author manu
 */
public abstract class ParseFileFromLocal
{
    private static Map<String,Integer>[] inside;//From String to Int, un Map por columna del Dataset
    private static Map<Integer,String>[] outside;//From Int to String, un Map por columna del Dataset
    private static String positiveClass;
    
    
    public static void parse(String fileName, String posClass, int classColNum,
            String splitBy) throws FileNotFoundException, IOException
    {
        File archivo = new File(fileName);
        FileReader fr = new FileReader (archivo);
        BufferedReader br = new BufferedReader(fr);
        String line, insertTmp, insertPositives = "",
                insertNegatives = "";
        String[] lineSplit;
        int numCol, numAtr, limit = Main.maxSizeStr, foldPositive = 1,
                foldNegative = 1;
        ParseFileFromLocal.positiveClass = posClass;
        classColNum--;
        long numPosClass = 0, numNegClass = 0;

        //Leemos una linea del fichero para poder crear las dos tablas
        line = br.readLine();
        lineSplit = line.split(splitBy);
        numAtr = lineSplit.length;
        numCol = numAtr+1;
        Main.numAttributes = numAtr;
        
        DataBase.dropTable(Main.nameBigTablePos);
        DataBase.dropTable(Main.nameBigTableNeg);
        DataBase.createBigTable(Main.nameBigTablePos, numCol);
        DataBase.createBigTable(Main.nameBigTableNeg, numCol);
        
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
        
        for (int i = 0, indexMap = 0; i < numAtr; i++, indexMap++)
        {
            if(i != classColNum)
            {
                //Si el valor es nuevo, se añade a las tablas hash para su traducción
                //a int
                if(!inside[indexMap].containsKey(lineSplit[i]))
                {
                    inside[indexMap].put(lineSplit[i], inside[indexMap].size()+1);
                    outside[indexMap].put(outside[indexMap].size()+1, lineSplit[i]);
                }

                insertTmp += inside[indexMap].get(lineSplit[i]) + ",";
            }
            else
            {
                //Si el valor es nuevo, se añade a las tablas hash para su traducción
                //a int
                if(!inside[numAtr-1].containsKey(lineSplit[i]))
                {
                    inside[numAtr-1].put(lineSplit[i], inside[numAtr-1].size()+1);
                    outside[numAtr-1].put(outside[numAtr-1].size()+1, lineSplit[i]);
                }
                
                indexMap--;
            }
        }
        
        insertTmp += inside[numAtr-1].get(lineSplit[classColNum]) + ",";
        
        if(posClass.equals(lineSplit[classColNum]))
        {
            insertTmp += foldPositive + ")";
            foldPositive++;
            insertPositives += insertTmp;
            numPosClass++;
        }
        else
        {
            insertTmp += foldNegative + ")";
            foldNegative++;
            insertNegatives += insertTmp;
            numNegClass++;
        }
        //--
        
        
        //Introducimos el dataset en el data warehouse
        while((line = br.readLine()) != null)
        {
            if(!line.equals(""))
            {
                lineSplit = line.split(splitBy);
                insertTmp = "(";

                for (int i = 0, indexMap = 0; i < numAtr; i++, indexMap++)
                {
                    if(i != classColNum)
                    {
                        //Si el valor es nuevo, se añade a las tablas hash para su traducción
                        //a int
                        if(!inside[indexMap].containsKey(lineSplit[i]))
                        {
                            inside[indexMap].put(lineSplit[i], inside[indexMap].size()+1);
                            outside[indexMap].put(outside[indexMap].size()+1, lineSplit[i]);
                        }

                        insertTmp += inside[indexMap].get(lineSplit[i]) + ",";
                    }
                    else
                    {
                        //Si el valor es nuevo, se añade a las tablas hash para su traducción
                        //a int
                        if(!inside[numAtr-1].containsKey(lineSplit[i]))
                        {
                            inside[numAtr-1].put(lineSplit[i], inside[numAtr-1].size()+1);
                            outside[numAtr-1].put(outside[numAtr-1].size()+1, lineSplit[i]);
                        }

                        indexMap--;
                    }
                }

                insertTmp += inside[numAtr-1].get(lineSplit[classColNum]) + ",";

                if(posClass.equals(lineSplit[classColNum]))
                {
                    insertTmp += foldPositive + ")";
                    foldPositive++;

                    if(foldPositive > 5) foldPositive = 1;

                    if(!insertPositives.equals("")) insertPositives += "," + insertTmp;

                    else insertPositives = insertTmp;

                    if(insertPositives.length() > limit)
                    {
                        DataBase.insert(Main.nameBigTablePos, insertPositives);
                        insertPositives = "";
                    }

                    numPosClass++;
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
                        DataBase.insert(Main.nameBigTableNeg, insertNegatives);
                        insertNegatives = "";
                    }

                    numNegClass++;
                }
                //--
            }
        }
        //--
        
        //String pos = ParseFileFromLocal.getBinaryPosClass();
        //String neg = ParseFileFromLocal.getBinaryNegClass();
        
        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$-> Final insert...");
        
        //Insertamos los datos restantes
        if(!insertPositives.equals(""))
            DataBase.insert(Main.nameBigTablePos, insertPositives);
        
        if(!insertNegatives.equals(""))
            DataBase.insert(Main.nameBigTableNeg, insertNegatives);
        //--
    }
    
    
    /**
     * De valor numérico a original de entrada tipo String
     * @param line
     * @return 
     */
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
    
    
    /**
     * De ejemplo binario a cadena de caracteres
     * @param line
     * @return 
     */
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
    
    
    /**
     * De regla en binario a regla con los datos de entrada
     * @param line
     * @return
     */
    public static String binaryRuleToString(String line)
    {
        //System.out.println(line);
        String[] valuesStr = line.split(",");
        String result = "{";
        
        //Por cada atributo
        for (int i = 0; i < valuesStr.length-1; i++)
        {
            char[] atr = valuesStr[i].toCharArray();
            int value = atr.length;
            String atrStr = "";
            
            //Por cada bit
            for (int j = 0; j < atr.length; j++)
            {
                if(atr[j] == '1')
                {
                    if(atrStr.equals(""))
                        atrStr = "(Att" + (i+1) + "=(";
                    
                    if(!atrStr.equals("(Att" + (i+1) + "=("))
                        atrStr += ",";
                    
                    atrStr += outside[i].get(value);
                }
                
                value--;
            }
            
            if(!atrStr.equals(""))
            {
                atrStr += "))";
                
                if(!result.equals("{"))
                    result += "^" + atrStr;

                else
                    result += atrStr;
            }
        }
        
        //Atributo de clase
        char[] atr = valuesStr[valuesStr.length-1].toCharArray();
        int j = 0;
        int value = atr.length;

        while(atr[j] != '1')
        {
            j++;
            value--;
        }

        
        result += "}=> (class=" +  outside[valuesStr.length-1].get(value) + ")";
        
        return result;
    }
    
    
    public static int[] getNumBits()
    {
        int[] numBits = new int[ParseFileFromLocal.inside.length];
        
        for (int i = 0; i < numBits.length; i++) numBits[i] = ParseFileFromLocal.inside[i].size();
        
        return numBits;
    }
    
    
    public static String getBinaryPosClass()
    {
        int[] tabNumBits = ParseFileFromLocal.getNumBits();
        int size = tabNumBits.length-1;
        int numBits = tabNumBits[size];//El atributo de clase es el último
        
        return ParseFileFromLocal.createBinaryValue(numBits, 
                ParseFileFromLocal.inside[size]
                        .get(ParseFileFromLocal.positiveClass));
    }
    
    
    public static String getBinaryNegClass()
    {
        int[] tabNumBits = ParseFileFromLocal.getNumBits();
        int size = tabNumBits.length-1;
        int numBits = tabNumBits[size];//El atributo de clase es el último
        Set<String> s = new HashSet(ParseFileFromLocal.inside[size].keySet());
        s.remove(ParseFileFromLocal.positiveClass);
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
