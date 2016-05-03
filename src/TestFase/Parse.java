/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TestFase;

import AGL.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Parsea las reglas
 * @author manu
 */
public abstract class Parse
{
    public static ArrayList<Rule> parseRules(String rulesString) throws FileNotFoundException, IOException
    {
        Attribute[] attributes;
        Attribute at;
        ArrayList<Rule> rules = new ArrayList();
        String[] rulesStr;

        rulesStr = rulesString.split("\t");

        //Construimos el rulesset
        for(String rule1 : rulesStr)
        {
            String[] atr = rule1.split(",");//Atributos de la regla
            attributes = new Attribute[atr.length];
            
            //Para cada atributo de la regla
            for (int j = 0; j < atr.length; j++)
            {
                at = new Attribute(atr[j].toCharArray());
                attributes[j] = at;
            }
            
            //añadimos el ejemplo al rulesset
            rules.add(new Rule(attributes));
        }
        
        return rules;
    }
    
    
    public static Example parseExample(String example)
    {
        Attribute[] attributes;
        Attribute at;

        String[] atr = example.split(",");//Atributos del ejemplo
        attributes = new Attribute[atr.length];

        //Para cada atributo del ejemplo
        for (int j = 0; j < atr.length; j++)
        {
            at = new Attribute(atr[j].toCharArray());
            attributes[j] = at;
        }
        
        return new Example(attributes);
    }
}
