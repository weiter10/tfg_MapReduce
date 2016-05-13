/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Hdfs_Operations;

/**
 *
 * @author manu
 */
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

public class HdfsRemove extends Configured implements Tool
{
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    public static BufferedWriter br;

    @Override
    public int run(String[] args) throws Exception
    {
        FileSystem fs = FileSystem.get(getConf());
        fs.delete(new Path(args[0]), true); // delete file, true for recursive 
        fs.close();
        
        return 0;
    }
    
}
