/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hive_JDBC_Operations;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author manu
 */
public abstract class HiveConnect
{
    public static Connection getConnection()
    {
        Connection conn = null;
        
        try
        {
            conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "", "");
            
        }catch (SQLException ex)
        {
            Logger.getLogger(HiveConnect.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        return conn;
    }
}
