/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hive_JDBC_Operations;

/**
 *
 * @author manu
 */
public abstract class Test
{
    public static void main(String[] args)
    {
        DataBase.doQuery("SELECT * FROM negativeexamples WHERE id >= 1500 and id <= 1515");
    }
}
