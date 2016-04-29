/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hive_JDBC_Operations;

import Driver_Operations.Driver;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author manu
 */
public abstract class DataBase
{
    private static String atr = null;
    private static String toDefineAtr = null;
    
    
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
    
    
    public static void createTable(String tableName, int numCol)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            String atrNum = "";
            atr = "";
            toDefineAtr = "";
            
            for (int i = 1; i < numCol; i++)
            {
                atrNum += "atr" + i + " INT, ";
                
                atr += ", atr" + i;
                
                toDefineAtr += ", ?";
            }
            
            stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName
                + " (" + atrNum + " id BIGINT)"
                + " ROW FORMAT DELIMITED"
                + " FIELDS TERMINATED BY ','"
                + " LINES TERMINATED BY '\n'"
                + " STORED AS TEXTFILE");
            
            con.close();
            
            System.out.println("Create OK");

        } catch (SQLException ex)
        {
            Logger.getLogger(Driver.class.getName()).log(Level.SEVERE, null, ex);
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
            
            System.out.println("Insert OK");
            
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
            String line;
            
            /*
            while (rs.next())
            {
                line = "mapId: " + rs.getInt(1);
                
                for (int i = 0; i < numFields-1; i++) line += "; atr" + i + ": " + rs.getInt(i+2);
                
                System.out.println(line);
            }
            */
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
            
            System.out.println("Drop OK");
            
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
            stmt.execute("LOAD DATA LOCAL INPATH '"+ fileName +"' INTO TABLE " + tableName);
            
            con.close();
            
            System.out.println("Load OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static void countRows(String tableName)
    {
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            System.out.println("number of tuples: " + rs.getString(1));
            
            con.close();
            
            System.out.println("Count OK");
            
        } catch (SQLException ex) {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    public static long getNumRows(String tableName)
    {        
        try
        {
            Connection con = HiveConnect.getConnection();
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TBLPROPERTIES " + tableName);
            rs.next();rs.next();rs.next();
            
            long numRows = Long.parseLong(rs.getString(2));
            
            con.close();
            
            return numRows;
            
        } catch (SQLException ex)
        {
            Logger.getLogger(DataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return -1;
    }
    
    
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
    
    
    public static String getDataBinaryFormat(String tableName, int[] numBits,long init, long end)
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
            
            while(rs.next())
            {
                //El id lo dejamos fuera
                for (int i = 1; i < columnsNumber; i++)
                {
                    binaryValue = DataBase.createBinaryValue(numBits[i-1], rs.getInt(i));
                        
                    data += binaryValue + ",";
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
    
    
    private static String createBinaryValue(int numBits, int posiTrue)
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
