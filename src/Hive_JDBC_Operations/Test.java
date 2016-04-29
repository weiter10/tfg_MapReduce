/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hive_JDBC_Operations;

import Driver_Operations.Driver;

/**
 *
 * @author manu
 */
public abstract class Test
{
    public static void main(String[] args)
    {
        int[] fold = {1,2,3,4,5};
        
        DataBase.createTrainingTable(Driver.nameBigTablePos, Driver.nameTableTrainingPos, fold);
    }
}
