/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Local_Storage_Operations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 *
 * @author manu
 */
public abstract class LocalStorageCreateFolder
{
    public static void run(String path) throws Exception
    {
        File dir = new File(path);
        dir.mkdir();
    }
}
