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
import java.io.BufferedWriter;
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

public class HdfsReader extends Configured implements Tool
{
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    public static BufferedReader br;
    public static FileSystem fs;
    
    @Override
    public int run(String[] args) throws Exception
    {
        
        if (args.length < 1)
        {
            System.err.println("HdfsReader [hdfs path]");
            System.exit(1);
        }
        
        Path inputPath = new Path(args[0]);
        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        
        fs = FileSystem.get(conf);
        
        br = new BufferedReader(new InputStreamReader(fs.open(inputPath)));
        /*
        while((line = br.readLine()) != null)
        {
                System.out.println(line);
        }
        */

        return 0;
    }
}
