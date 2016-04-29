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
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HdfsReaderExt extends Configured implements Tool
{
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    
    @Override
    public int run(String[] args) throws Exception
    {
        
        if (args.length < 2)
        {
            System.err.println("HdfsReader [hdfs input path] [local output path]");
            return 1;
        }
        
        String line;
        Path inputPath = new Path(args[0]);
        String localOutputPath = args[1];
        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        
        FileSystem fs = FileSystem.get(conf);
        InputStream is = fs.open(inputPath);
        OutputStream os = new BufferedOutputStream(new FileOutputStream(localOutputPath));
        IOUtils.copyBytes(is, os, conf);
        
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
        /*
        System.out.println("$$$$$$$$$$$$$$$$$$$$$-> Results");
        
        while((line = br.readLine()) != null)
        {
                System.out.println(line);
        }
        */
        return 0;
    }
}
