/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ShellCommands;

/**
 *
 * @author manu
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class OutputRedirector extends Thread
{

    InputStream is;
    String type;

    public OutputRedirector(InputStream is, String type){
        this.is = is;
        this.type = type;
    }
    
    @Override
    public void run()
    {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println(type + "> " + line);
            }
        } catch (IOException ioE) {

        }
    }

}
