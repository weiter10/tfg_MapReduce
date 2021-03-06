/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hive_JDBC_Operations;

import Main_Operations.Main;
import Hdfs_Operations.HdfsWriter;
import Local_Storage_Operations.LocalStorageWrite;
import Parse.ParseFileFromLocal;
import java.io.BufferedWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author manu
 */
public abstract class DataBase
{
    private static String atr = null;
    private static String toDefineAtr = null;
    private static String atrNum = null;
    
    public static void describeAllTables()
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String show = "show tables", tableName, describe;
            ResultSet rs1 = stmt.executeQuery(show), rs2;
            
            while (rs1.next())
            {
                tableName = rs1.getString(1);
                describe = "describe " + tableName;
                System.out.println("Table Description: " + tableName);
                rs2 = stmt.executeQuery(describe);
                
                while (rs2.next())
                {
                    System.out.println(rs2.getString(1) + "\t" + rs2.getString(2));
                }
            }
            
            con.close();
            
            /*
            // Query to show tables
            String show = "show tables";
            System.out.println("Running: " + show);
            rs = stmt.executeQuery(show);
            if (rs.next()) {
                System.out.println(rs.getString(1));
            }

            // Query to describe table
            String describe = "describe " + tableName;
            System.out.println("Running: " + describe);
            rs = stmt.executeQuery(describe);
            while (rs.next()) {
                System.out.println(rs.getString(1) + "\t" + rs.getString(2));
            }
            */
            
        }catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void createBigTable(String tableName, int numCol)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            atrNum = "";
            atr = "";
            toDefineAtr = "";
            
            for (int i = 1; i < numCol; i++)
            {
                atrNum += "atr" + i + " INT, ";
                
                atr += ", atr" + i;
                
                toDefineAtr += ", ?";
            }
            
            stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName
                + " (" + atrNum + " fold INT)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY ','"
                + " LINES TERMINATED BY '\n'"
                + " STORED AS TEXTFILE");
            
            con.close();
            
            System.out.println("Create table " + tableName + " OK");

        } catch (SQLException ex)
        {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Crea una tabla con el nombre recibido + "Training" para training a partir
     * de la tabla indicada como parámetro y con los trozos indicados
     * @param source
     * @param tableName
     * @param folds 
     */
    public static void createTrainingTable(String source, String tableName, int[] folds)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query, insertTmp, insert = null;
            int limit = Main.maxSizeStr;
            long count = 1;
            
            query = "SELECT " + atr.substring(1) + " FROM " + source + " WHERE";
            
            //Contruimos la query
            for (int i = 0; i < folds.length; i++)
            {
                if(i == 0) query += " fold = " + folds[i];
                
                else query += " or fold = " + folds[i];
            }
            
            //Ejecutamos la consulta y nos traemos los datos
            
            DataBase.dropTable(tableName);
            
            stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName
                + " (" + atrNum + " id BIGINT)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY ','"
                + " LINES TERMINATED BY '\n'"
                + " STORED AS TEXTFILE");
            
            ResultSet rs = stmt.executeQuery(query);
            
            //Copiamos los datos a la nueva tabla
            while (rs.next())
            {
                insertTmp = "(";
                
                for (int i = 1; i <= Main.numAttributes; i++)
                    insertTmp += rs.getInt(i) + ",";
                
                insertTmp += count + ")";
                count++;
                
                if(insert == null) insert = insertTmp;
                
                else insert += "," + insertTmp;
                
                //Si el String ha alcazado un tamaño considerable lo guardamos
                //en la BD
                if(insert.length() > limit)
                {
                    DataBase.insert(tableName, insert);
                    insert = null;
                }
            }
            
            //Grabamos los datos restantes
            if(insert != null)
                DataBase.insert(tableName, insert);
            
            con.close();
            
            System.out.println("Create training table " + tableName + " OK");

        } catch (SQLException ex)
        {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Genera los ficheros de test set
     * @param sourceTestSetPos
     * @param sourceTestSetNeg
     * @param testFold
     * @param numBits
     * @throws IOException 
     */
    public static void writeTestSet(String sourceTestSetPos, String sourceTestSetNeg,
            int testFold, int[] numBits)
            throws IOException, Exception
    {
        try
        {
            //Escribimos los ejemplos positivos
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query = "SELECT * FROM " + sourceTestSetPos + " WHERE fold = " + testFold;
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            String data = "", binaryValue;
            int limit = Main.maxSizeStr, count = 0;
            
            while(rs.next())
            {
                //El número de fold lo dejamos fuera
                for (int i = 1; i < columnsNumber; i++)
                {
                    binaryValue = ParseFileFromLocal.createBinaryValue(numBits[i-1], rs.getInt(i));
                    data += binaryValue + ",";
                }
                
                data += "\t";
                
                if(data.length() > limit)
                {
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {Main.pathFolderTestFile + "/" + count});
                    BufferedWriter bw = HdfsWriter.bw;
                    count++;
                    
                    //Escribimos la clase minoritaria
                    bw.write(ParseFileFromLocal.getBinaryPosClass()+ "\t\t");
                    //Escribimos la clase mayoritaria
                    bw.write(ParseFileFromLocal.getBinaryNegClass()+ "\t\t");
                    //Escribimos los ejemplos
                    bw.write(data);
                    bw.write("\n");
                    data = "";
                    bw.close();
                }
            }
            
            //Escribimos los ejemplos negativos
            query = "SELECT * FROM " + sourceTestSetNeg + " WHERE fold = " + testFold;
            rs = stmt.executeQuery(query);
            
            while(rs.next())
            {
                //El número de fold lo dejamos fuera
                for (int i = 1; i < columnsNumber; i++)
                {
                    binaryValue = ParseFileFromLocal.createBinaryValue(numBits[i-1], rs.getInt(i));
                    
                    data += binaryValue + ",";
                }
                
                data += "\t";
                
                if(data.length() > limit)
                {
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {Main.pathFolderTestFile + "/" + count});
                    BufferedWriter bw = HdfsWriter.bw;
                    count++;
                    
                    //Escribimos la clase minoritaria
                    bw.write(ParseFileFromLocal.getBinaryPosClass() + "\t\t");
                    //Escribimos la clase mayoritaria
                    bw.write(ParseFileFromLocal.getBinaryNegClass()+ "\t\t");
                    //Escribimos los ejemplos
                    bw.write(data);
                    bw.write("\n");
                    data = "";
                    bw.close();
                }
            }
            
            //Obtenemos el buffer de escritura en HDFS
            ToolRunner.run(new HdfsWriter(), new String[] {Main.pathFolderTestFile + "/" + count});
            BufferedWriter bw = HdfsWriter.bw;

            //Escribimos la clase minoritaria
            bw.write(ParseFileFromLocal.getBinaryPosClass() + "\t\t");
            //Escribimos la clase mayoritaria
            bw.write(ParseFileFromLocal.getBinaryNegClass()+ "\t\t");
            //Escribimos los ejemplos
            bw.write(data);
            bw.write("\n");
            bw.close();
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void insert(String tableName, String data)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            /*
            PreparedStatement ps = con.prepareStatement(
                    "insert into "+ tableName +" (mapid "+ atr + ") " + 
                    "VALUES (?" + toDefineAtr +")");
            
            ps.setInt(1, mapid);
            
            for (int i = 0; i < example.length; i++) ps.setInt(i+2, example[i]);
            
            ps.executeUpdate();
            */
            
            stmt.execute("insert into "+ tableName + " VALUES " + data);
            
            con.close();
            
            System.out.println("Insert in table " + tableName + " OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void show(String tableName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            System.out.println("Num Col: " + columnsNumber);
            rs.next();
            for (int i = 1; i <= columnsNumber; i++)
            {
                System.out.println(rs.getString(i));
            }
            
            con.close();
            
            System.out.println("Show OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void doQuery(String query)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            String line;
            
            
            while (rs.next())
            {
                line="";
                
                for (int i = 1; i <= columnsNumber; i++) line += rs.getString(i) + "\t";
                
                System.out.println(line);
            }
            
            con.close();
            
            System.out.println("Query OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }    
    
    
    public static void dropTable(String tableName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("DROP TABLE IF EXISTS " + tableName);
            
            con.close();
            
            System.out.println("Drop table " + tableName + " OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void createRuleTable(String tableName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            
            DataBase.dropTable(tableName);
            
            stmt.execute("CREATE TABLE " + tableName
                + " (rule STRING,"
                + "fieldNull INT,"
                + " numRe INT,"
                + " numPos INT,"
                + " numNeg INT,"
                + " piM DOUBLE)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY '\t'"
                + " LINES TERMINATED BY '\n'"
                + " STORED AS TEXTFILE");
            
            con.close();
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void load(String tableName, String fileName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            stmt.execute("LOAD DATA INPATH '"+ fileName +"' INTO TABLE " + tableName);
            
            con.close();
            
            System.out.println("Load in " + tableName + " OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Escribe en el fichero las reglas almacenadas en la tabla.
     * @param tableName
     * @param bw
     * @throws IOException 
     */
    public static void writeOrderedRulesInFile(String tableName, BufferedWriter bw) throws IOException
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query = "SELECT rule, piM FROM " + tableName + " ORDER BY piM DESC";
            ResultSet rs = stmt.executeQuery(query);
            String data = "";
            
            while(rs.next())
            {
                data += rs.getString(1) + "\t";
                
                if(data.length() > Main.maxSizeStr)
                {
                    bw.write(data);
                    data = "";
                }
            }
            
            bw.write(data);
            
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Escribe las reglas traducidar a un fichero local
     * @param tableName
     * @param fileName
     * @throws IOException 
     */
    public static void writeRulesInLocalFile(String tableName, String fileName)
            throws IOException, Exception
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query = "SELECT * FROM " + tableName + " ORDER BY piM DESC";
            ResultSet rs = stmt.executeQuery(query);
            String binaryRule;
            String rules = "", tmp;
            int i = 1;
            ArrayList<String> rulesStr = new ArrayList();
            
            while(rs.next())
            {
                binaryRule = rs.getString("rule");
                tmp = ParseFileFromLocal.binaryRuleToString(binaryRule);
                rulesStr.add("Rule" + i + tmp + "\n"
                        + "[Num. repeticiones:" + rs.getString("numRe") + "]"
                        + "[Num. positivos:" + rs.getString("numPos") + "]"
                        + "[Num. negativos:" + rs.getString("numNeg") + "]"
                        +"[PI: " + rs.getString("piM") + "]\n\n");
                i++;
            }
            
            LocalStorageWrite.run(fileName, rulesStr);
            
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Escribe en el fichero las reglas almacenadas en la tabla. Las reglas se escriben
     * en una linea y separadas por tabulaciones
     * @param tableName
     * @param bw
     * @throws IOException 
     */
    public static void writeBulkRulesInFile(String tableName, BufferedWriter bw) throws IOException
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query = "SELECT rule FROM " + tableName;
            ResultSet rs = stmt.executeQuery(query);
            String data = "";
            
            while(rs.next())
            {
                data += rs.getString(1) + "\t";
                
                if(data.length() > Main.maxSizeStr)
                {
                    bw.write(data);
                    data = "";
                }
            }
            
            bw.write(data);
            
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    
    public static long getNumRows(String tableName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String query = "SELECT COUNT(*) FROM " + tableName;
            ResultSet rs = stmt.executeQuery(query);
            rs.next();
            long numRows = Long.parseLong(rs.getString(1));
            
            con.close();
            
            return numRows;
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return -1;
    }
    
    
    @Deprecated
    public static String getData(String tableName, long init, long end)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE "
                    + "id >= " + init + " and id <= " + end);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            String data = "";
            
            while(rs.next())
            {
                //El id lo dejamos fuera
                for (int i = 1; i < columnsNumber; i++)
                {
                    data += rs.getInt(i) + ",";
                }
                
                data += "\t";
            }
            
            con.close();
            
            return data;
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return null;
    }
    
    /**
     * Escribe en el fichero los datos especificados por parámetro
     * @param tableName
     * @param numBits
     * @param init
     * @param end
     * @param bw
     * @throws IOException 
     */
    public static void writeDataBinaryFormat(String tableName, int[] numBits, 
            long init, long end, BufferedWriter bw) throws IOException
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE "
                    + "id >= " + init + " and id <= " + end);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            String data = "", binaryValue;
            int limit = Main.maxSizeStr;
            
            while(rs.next())
            {
                //El id lo dejamos fuera
                for (int i = 1; i < columnsNumber; i++)
                {
                    binaryValue = ParseFileFromLocal.createBinaryValue(numBits[i-1], rs.getInt(i));
                        
                    data += binaryValue + ",";
                }
                
                data += "\t";
                
                if(data.length() > limit)
                {
                    bw.write(data);
                    data = "";
                }
            }
            
            bw.write(data);
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Genera ficheros con datos de training seleccionando alternativamente
     * uno de la clase positiva y otro de la negativa.
     * @param tablePos
     * @param tableNeg
     * @param fileName
     * @param numBits
     * @throws IOException
     * @throws Exception 
     */    
    public static void writeBalancedDataBinaryFormat(String tablePos, 
            String tableNeg, String fileName, int[] numBits) throws IOException, Exception
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt1 = con.createStatement(), stmt2 = con.createStatement();
            ResultSet[] rs = new ResultSet[2];
            rs[0] = stmt1.executeQuery("SELECT * FROM " + tablePos);
            rs[1] = stmt2.executeQuery("SELECT * FROM " + tableNeg);
            ResultSetMetaData rsmd = rs[0].getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            long sizePos = DataBase.getNumRows(tablePos);
            long sizeNeg = DataBase.getNumRows(tableNeg);
            String data = "", binaryValue;
            int countFiles = 0, indexSmall, indexBig;
            
            if(sizePos > sizeNeg)
            {
                indexBig = 0;
                indexSmall = 1;
            }
            else
            {
                indexBig = 1;
                indexSmall = 0;
            }

            while(rs[indexSmall].next())
            {
                rs[indexBig].next();

                for (ResultSet rs1 : rs)
                {
                    //El id lo dejamos fuera
                    for (int j = 1; j < columnsNumber; j++)
                    {
                        binaryValue = ParseFileFromLocal.createBinaryValue(numBits[j-1], rs1.getInt(j));

                        data += binaryValue + ",";
                    }

                    data += "\t";
                }

                if(data.length() > Main.maxSizeStr)
                {
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                    BufferedWriter bw = HdfsWriter.bw;
                    countFiles++;

                    //Escribimos la semilla aleatoria y el IR
                    bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                    Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                    bw.write(data);
                    Main.countSeedRnd++;
                    bw.close();
                    data = "";
                }
            }

            if(indexSmall == 1)
                rs[indexSmall] = stmt2.executeQuery("SELECT * FROM " + tableNeg);
            
            else
                rs[indexSmall] = stmt1.executeQuery("SELECT * FROM " + tablePos);

            while(rs[indexBig].next())
            {
                rs[indexSmall].next();

                for (ResultSet rs1 : rs)
                {
                    //El id lo dejamos fuera
                    for (int j = 1; j < columnsNumber; j++)
                    {
                        binaryValue = ParseFileFromLocal.createBinaryValue(numBits[j-1], rs1.getInt(j));

                        data += binaryValue + ",";
                    }

                    data += "\t";
                }

                if(data.length() > Main.maxSizeStr)
                {
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                    BufferedWriter bw = HdfsWriter.bw;
                    countFiles++;

                    //Escribimos la semilla aleatoria y el IR
                    bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                    Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                    bw.write(data);
                    Main.countSeedRnd++;
                    bw.close();
                    data = "";
                }
            }
            
            if(!data.equals(""))
            {
                //Obtenemos el buffer de escritura en HDFS
                ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                BufferedWriter bw = HdfsWriter.bw;
                countFiles++;

                //Escribimos la semilla aleatoria y el IR
                bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                        Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                bw.write(data);
                Main.countSeedRnd++;
                bw.close();
            }
            
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    /**
     * Sobrecarga, este método genera tantos ficheros como tamaño se especifica
     * en numFiles
     * @param tablePos
     * @param tableNeg
     * @param fileName
     * @param numBits
     * @param numFiles
     * @throws IOException
     * @throws Exception 
     */
    public static void writeBalancedDataBinaryFormat(String tablePos, 
            String tableNeg, String fileName, int[] numBits, int numFiles)
            throws IOException, Exception
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt1 = con.createStatement(), stmt2 = con.createStatement();
            ResultSet[] rs = new ResultSet[2];
            rs[0] = stmt1.executeQuery("SELECT * FROM " + tablePos);
            rs[1] = stmt2.executeQuery("SELECT * FROM " + tableNeg);
            ResultSetMetaData rsmd = rs[0].getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            long sizePos = DataBase.getNumRows(tablePos);
            long sizeNeg = DataBase.getNumRows(tableNeg);
            String data = "", binaryValue;
            int countFiles = 0, indexSmall, indexBig, countExamples = 0;
            int maxExamples;
            
            if(sizePos > sizeNeg)
            {
                indexBig = 0;
                indexSmall = 1;
                maxExamples = (int) Math.ceil((double)sizePos/numFiles);
            }
            else
            {
                indexBig = 1;
                indexSmall = 0;
                maxExamples = (int) Math.ceil((double)sizeNeg/numFiles);
            }
            
            System.out.println("Limite: " + maxExamples);

            while(rs[indexSmall].next())
            {
                rs[indexBig].next();

                for (ResultSet rs1 : rs)
                {
                    //El id lo dejamos fuera
                    for (int j = 1; j < columnsNumber; j++)
                    {
                        binaryValue = ParseFileFromLocal.createBinaryValue(numBits[j-1], rs1.getInt(j));

                        data += binaryValue + ",";
                    }

                    data += "\t";
                }
                
                countExamples++;

                if(countExamples >= maxExamples)
                {
                    System.out.println("Grabando1......................");
                    countExamples = 0;
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                    BufferedWriter bw = HdfsWriter.bw;
                    countFiles++;

                    //Escribimos la semilla aleatoria y el IR
                    bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                    Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                    bw.write(data);
                    Main.countSeedRnd++;
                    bw.close();
                    data = "";
                }
            }
            
            System.out.println("Contador1: " + countExamples);

            if(indexSmall == 1)
                rs[indexSmall] = stmt2.executeQuery("SELECT * FROM " + tableNeg);
            
            else
                rs[indexSmall] = stmt1.executeQuery("SELECT * FROM " + tablePos);

            while(rs[indexBig].next())
            {
                if(!rs[indexSmall].next())
                {
                    if(indexSmall == 1)
                        rs[indexSmall] = stmt2.executeQuery("SELECT * FROM " + tableNeg);

                    else
                        rs[indexSmall] = stmt1.executeQuery("SELECT * FROM " + tablePos);
                    
                    rs[indexSmall].next();
                }

                for (ResultSet rs1 : rs)
                {
                    //El id lo dejamos fuera
                    for (int j = 1; j < columnsNumber; j++)
                    {
                        binaryValue = ParseFileFromLocal.createBinaryValue(numBits[j-1], rs1.getInt(j));

                        data += binaryValue + ",";
                    }

                    data += "\t";
                }
                
                countExamples++;

                if(countExamples >= maxExamples)
                {
                    System.out.println("Grabando2......................");
                    countExamples = 0;
                    //Obtenemos el buffer de escritura en HDFS
                    ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                    BufferedWriter bw = HdfsWriter.bw;
                    countFiles++;

                    //Escribimos la semilla aleatoria y el IR
                    bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                    Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                    bw.write(data);
                    Main.countSeedRnd++;
                    bw.close();
                    data = "";
                }
            }
            
            System.out.println("Contador2: " + countExamples);
            
            if(!data.equals(""))
            {
                //Obtenemos el buffer de escritura en HDFS
                ToolRunner.run(new HdfsWriter(), new String[] {fileName + countFiles});
                BufferedWriter bw = HdfsWriter.bw;
                countFiles++;

                //Escribimos la semilla aleatoria y el IR
                bw.write(Main.countSeedRnd + "\t\t" + 1 + "\t\t" +
                        Main.algorithmSizeP + "\t\t" + Main.algorithmIter + "\t\t");
                bw.write(data);
                Main.countSeedRnd++;
                bw.close();
            }
            
            con.close();
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
