/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;

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
 *
 * @author manu
 */
public final class Parse
{
    private final ArrayList<Example> data;//Almacena el dataset completo
    private Map<Attribute,Set<Integer>>[] mapAtr = null;//Almacena, para cada atributo,
    //en que ejemplos se dan los valores
    private final Set<Integer> validExamples;//Almacena los índices de los ejemplos 
    //válidos (aun no cubiertos por ninguna regla). Atención, el map del atributo de 
    //clase disminuye conforme se van cubriendo ejemplos.
    
    private float iR;

    public Set<Integer> getValidExamples()
    {
        return validExamples;
    }

    public ArrayList<Example> getData()
    {
        return data;
    }

    public Map<Attribute, Set<Integer>>[] getMapAtr()
    {
        return mapAtr;
    }
    
    public Parse(String dataString) throws FileNotFoundException, IOException
    {
        Attribute[] attributes;
        Attribute at;
        String[] examples;
        this.data = new ArrayList();
        this.validExamples = new HashSet();

        examples = dataString.split("\t");
        iR = Float.parseFloat(examples[0]);
        //Construimos el dataset
        for(int i = 1; i < examples.length; i++)
        {
            String example1 = examples[i];
            String[] atr = example1.split(",");//Atributos del ejemplos
            attributes = new Attribute[atr.length];
            
            if(mapAtr == null)
            {
                mapAtr = new HashMap[atr.length];
                
                for (int j = 0; j < mapAtr.length; j++) mapAtr[j] = new HashMap();
            }
            
            //Para cada atributo del ejemplo
            for (int j = 0; j < atr.length; j++)
            {
                at = new Attribute(atr[j].toCharArray());
                
                if(!mapAtr[j].containsKey(at)) mapAtr[j].put(at, new HashSet());
                
                //Añadimos indice del ejemplo con el valor del atributo
                mapAtr[j].get(at).add(data.size());
                attributes[j] = at;
            }
            
            //añadimos el ejemplo al dataset
            this.data.add(new Example(attributes));
        }

        this.resetValidExamples();
    }
    
    
    public void resetValidExamples()
    {
        this.validExamples.clear();
        for (int i = 0; i < data.size(); i++) this.validExamples.add(i);
    }
    
    
    /**
     * Quita de los ejemplos válidos los cubiertos por la regla "rule" 
     * @param rule 
     */
    public void removeCoverExamples(Rule rule)
    {
        Set<Integer> coveredExamples = new HashSet();
        Example example;
        
        for(int index : this.validExamples)
        {
            example = this.data.get(index);
            
            if(rule.coverExample(example) == 1) coveredExamples.add(index);
        }
        
        //Quitamos los ejemplos del conjunto general
        this.validExamples.removeAll(coveredExamples);
        Map<Attribute,Set<Integer>> mapClass = this.mapAtr[this.mapAtr.length-1];
        //Quitamos los ejemplos del conjunto al que clasifica la regla (clase)
        mapClass.get(rule.getClassAttribute()).removeAll(coveredExamples);
    }
    
    /**
     * Devuelve los conjuntos donde almacenan los ejemplos válidos para cada clase
     * @return 
     */
    public ArrayList<ArrayList<Integer>> getValidExamplesByClasses()
    {
        Map<Attribute,Set<Integer>> mapClass = this.mapAtr[this.mapAtr.length-1];
        Collection<Set<Integer>> col = mapClass.values();
        ArrayList<ArrayList<Integer>> result = new ArrayList();
        
        for(Set<Integer> s : col) result.add(new ArrayList(s));
        
        return result;
    }
}
