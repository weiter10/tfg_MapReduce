/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ShellCommands;

import java.io.IOException;
import java.lang.ProcessBuilder;
import java.util.Map;

//This Works

public class ConsoleTester {

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException
    {

        ProcessBuilder hiveProcessBuilder = new ProcessBuilder("echo $PATH");
        //String path = processEnv.get("PATH");
        Process hiveProcess = hiveProcessBuilder.start();

        OutputRedirector outRedirect = new OutputRedirector(
                hiveProcess.getInputStream(), "HIVE_OUTPUT");
        OutputRedirector outToConsole = new OutputRedirector(
                hiveProcess.getErrorStream(), "HIVE_LOG");

        outRedirect.start();
        outToConsole.start();    
    }

}
