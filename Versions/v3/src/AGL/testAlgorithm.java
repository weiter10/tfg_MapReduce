/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author manu
 */
public class testAlgorithm
{
    public static void main(String[] args) throws IOException
    {
        Random rnd = new Random();
        rnd.setSeed(0);
        
        /*
        Parse parse = new Parse("/home/manu/Dropbox/gradoInformatica/4 Cuarto/2 Cuatrimestre/TFG/Dataset/datasetHDFS");
        
        //Pruebas clase Attribute
        char[] c1 = new char[3];
        c1[0] = '1';
        c1[1] = '0';
        c1[2] = '1';
        Attribute a1 = new Attribute(c1);
        
        char[] c2 = new char[2];
        c2[0] = '0';
        c2[1] = '1';
        Attribute a2 = new Attribute(c2);
        
        char[] c3 = new char[2];
        c3[0] = '0';
        c3[1] = '1';
        Attribute ac = new Attribute(c3);
        
        Attribute[] atr1 = new Attribute[3];
        atr1[0] = a1;
        atr1[1] = a2;
        atr1[2] = ac;
        
        Rule r1 = new Rule(atr1, parse);
        Rule r2 = new Rule(r1);
        //r1.setValue(0, '0');
        r2.setValue(6, '0');
        
        Map<Rule,Rule> s = new HashMap();
        s.put(r1,r1);
        s.put(r2,r2);
        //--
        */
        
        
        //File archivo = new File("/home/manu/Dropbox/gradoInformatica/4 Cuarto/2 Cuatrimestre/TFG/Dataset/datasetHDFS");
        File archivo = new File(args[0]);
        FileReader fr = new FileReader (archivo);
        BufferedReader br = new BufferedReader(fr);
        
        Algorithm alg = new Algorithm(br.readLine(), null);
        PriorityQueue<Rule> p = new PriorityQueue();
        p.addAll(alg.run());
        
        int size = p.size();
        
        while(!p.isEmpty())
            System.out.println(p.poll().show());
        
        System.out.println("Number of rules: " + size);
        
        
        int nada = 0;
    }
}
