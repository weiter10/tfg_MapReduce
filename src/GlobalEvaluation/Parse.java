/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GlobalEvaluation;

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
 * Parsea los ejemplos y las reglas
 * @author manu
 */
public class Parse
{
    private final Set<Rule> rules;
    private final Set<Example> examples;
    
    public Parse(String examplesBulk, String rulesBulk) throws FileNotFoundException, IOException
    {
        Attribute[] attributes;
        Attribute at;
        rules = new HashSet();
        examples = new HashSet();
        String[] rulesStr, examplesStr;

        examplesStr = examplesBulk.split("\t");
        rulesStr = rulesBulk.split("\t");

        
        //Contruimos el dataset
        for(String example1 : examplesStr)
        {
            String[] atr = example1.split(",");//Atributos
            attributes = new Attribute[atr.length];
            
            //Para cada atributo
            for (int i = 0; i < atr.length; i++)
            {
                at = new Attribute(atr[i].toCharArray());
                attributes[i] = at;
            }
            
            //añadimos el ejemplo al dataset
            examples.add(new Example(attributes));
        }
        
        
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
            
            rules.add(new Rule(attributes, examples));
        }
    }

    public Set<Rule> getRules() {
        return rules;
    }

    public Set<Example> getExamples() {
        return examples;
    }
    
    
}
