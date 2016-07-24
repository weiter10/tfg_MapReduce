/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Local_Storage_Operations;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

/**
 *
 * @author manu
 */
public abstract class LocalStorageWrite
{
    private static BufferedWriter bw;
    
    public static void run(String fileName, String data) throws Exception
    {
        File file = new File(fileName);
        file.createNewFile();
        bw = new BufferedWriter(new FileWriter(file));
        bw.write(data);
        bw.flush();
        bw.close();
    }
    
    
    public static void run(String fileName, ArrayList<String> data) throws Exception
    {
        File file = new File(fileName);
        file.createNewFile();
        bw = new BufferedWriter(new FileWriter(file));
        
        for(String line : data)
            bw.write(line);
        
        bw.flush();
        bw.close();
    }
}
