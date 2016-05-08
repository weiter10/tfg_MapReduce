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

public class HdfsWriter extends Configured implements Tool
{
    public static final String FS_PARAM_NAME = "fs.defaultFS";
    public static BufferedWriter br;
    public static FileSystem fs;

    @Override
    public int run(String[] args) throws Exception
    {
        Path outputPath = new Path(args[0]);

        Configuration conf = getConf();
        System.out.println("configured filesystem = " + conf.get(FS_PARAM_NAME));
        HdfsWriter.fs = FileSystem.get(conf);
        
        /*
        if (fs.exists(outputPath))
        {
            System.err.println("output path exists");
            return 1;
        }
        */
        
        HdfsWriter.br = new BufferedWriter(new OutputStreamWriter(fs.create(outputPath,true)));
                                   // TO append data to a file, use fs.append(Path f)
        return 0;
    }
}
