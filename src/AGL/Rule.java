/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author manu
 */
public final class Rule extends Row implements Comparable
{
    private double evaluationFunction = -1, pi = -1;
    private int positives, negatives;
    private final Parse parse;
    
    
    public Rule(Attribute[] row, Parse data)
    {
        super(row);
        this.parse = data;
        this.updatePerformance();
    }
    
    
    public Rule(Rule paramRule)
    {
        super(paramRule);
        this.parse = paramRule.parse;
        this.updatePerformance();
    }
    
    /**
     * Actualiza la función de evaluación y el pi de la regla
     */
    public void updatePerformance()
    {
        Example example;
        int value;
        double valueD;
        Set<Integer> validExamples = parse.getValidExamples();
        ArrayList<Example> data = parse.getData();
        negatives = 0;
        positives = 0;
        
        //Testeamos la regla con todos los ejemplos aún válidos
        for(int index : validExamples)
        {
            example = data.get(index);
            value =  this.coverExample(example);
            
            if(value == 1) positives++;
            
            else if(value == -1) negatives++;
        }
        
        valueD = 1 + ((double)this.getNumZeros()/(this.getSizeChromosome()-this.getClassAttribute().size()));
        evaluationFunction = Math.pow(valueD, -negatives);
        pi = positives*evaluationFunction;
    }

    public double getEvaluationFunction()
    {
        return evaluationFunction;
    }

    public double getPi()
    {
        return pi;
    }
    
    @Override
    public int compareTo(Object obj)
    {
        Rule r = (Rule) obj;
        
        if(this.pi > r.pi) return 1;
        
        if(this.pi < r.pi) return -1;
        
        //En caso de tener el mismo pi, ordenador por la función de evaluación
        if(this.evaluationFunction > r.evaluationFunction) return 1;
        
        if(this.evaluationFunction < r.evaluationFunction) return -1;
        
        return 0;
    }
    
    
    /**
     * Devuelve 1 si la regla cubre al ejemplo y clasifica a la misma clase,
     * 0 si no la cubre, y -1 si la regla cubre al ejemplo y clasifica a una
     * clase distinta
     * @param ex
     * @return 
     */
    public int coverExample(Example ex)
    {
        for (int i = 0; i < super.row.length-1; i++)
        {
            if(!this.row[i].coverAttribute(ex.row[i])) return 0;
        }
        
        if(this.row[row.length-1].sameClass(ex.row[row.length-1])) return 1;
        
        return -1;
    }
    
    
    /**
     * Modifica la regla para cubrir al ejemplo ex
     * @param ex 
     */
    public void modifyToCover(Example ex)
    {
        for (int i = 0; i < super.row.length; i++)
        {
            this.row[i].modifyToCover(ex.row[i]);
        }
        
        this.updatePerformance();
    }
    
    /**
     * Cambiar el valor de la posición "posi" por "value"
     * Atención: este método no actualiza el performance de la regla!
     * El usuario tendrá que llamar a "updatePerformance()" después de aplicar
     * este método para que la regla tenga actualizado su performance
     * @param posi
     * @param value 
     */
    public void setValue(int posi, char value)
    {
        this.indexAtr.get(posi).setValue(this.indexPos.get(posi), value);
    }
    
    
    /**
     * Muta la posición indicada por parámetro
     * @param posi 
     */
    public void mutate(int posi)
    {
        if(this.getValue(posi) == '0') this.setValue(posi, '1');
        
        else this.setValue(posi, '0');
        
        this.updatePerformance();
    }
    
    /**
     * Cambiar el atributo de clase por el especificado por parámetro
     * @param classAtr 
     */
    public void setClassAttribute(Attribute classAtr)
    {
        row[row.length-1] = new Attribute(classAtr);
        
        this.updatePerformance();
    }
    
    /**
     * Comprueba si una regla es trivial, es decir, su cromosoma está compuesto
     * solo por '1' o '0'
     * @return 
     */
    public boolean validChromosome()
    {
        int sizeChromosome = this.getSizeChromosome()-this.getClassAttribute().size();
   
        return !(sizeChromosome == this.getNumZeros() || this.getNumZeros() == 0);
    }
    
    
    /**
     * Genera una regla aleatoria
     * @param rnd
     * @param pattern
     * @param atrClass
     * @param parse
     * @return 
     */
    public static Rule generateRandomRule(Random rnd, int[] pattern,
            Attribute atrClass, Parse parse)
    {
        Attribute[] attributes = new Attribute[pattern.length];
        char[] atr;
        
        //Obtener la regla aleatoria menos el attributo de clase
        for (int i = 0; i < pattern.length-1; i++)
        {
            atr = new char[pattern[i]];
            
            for (int j = 0; j < atr.length; j++)
            {
                if(rnd.nextInt(2) == 0) atr[j] = '0';
                
                else atr[j] = '1';
            }
            
            attributes[i] = new Attribute(atr);
        }
        
        //Asignamos la clase, llamamos al constructor de copia del atributo de clase
        //del ejemplo
        attributes[pattern.length-1] = new Attribute(atrClass);
        
        return new Rule(attributes, parse);
    }
    
    
    /**
     * Genera una regla aleatoria
     * @param rnd
     * @param pattern
     * @param parse
     * @return 
     */
    public static Rule generateRandomRule(Random rnd, int[] pattern, Parse parse)
    {
        char[] atr;
        int index;
        
        //Asignamos la clase
        atr = new char[pattern[pattern.length-1]];
        index = rnd.nextInt(atr.length);
        
        for (int i = 0; i < atr.length; i++) 
        {
            if(i != index) atr[i] = '0';
            
            else atr[i] = '1';
        }
        
        return Rule.generateRandomRule(rnd, pattern, new Attribute(atr), parse);
    }
    
    
    @Override
    public String toString()
    {
        return super.toString();
    }
    
    
    public static Comparator<Rule> comparatorByEF()
    {
        return new Comparator()
        {
            @Override
            public int compare(Object o1, Object o2)
            {
                Rule r1 = (Rule) o1, r2 = (Rule) o2;
                
                double eFr1 = r1.getEvaluationFunction(),
                        eFr2 = r2.getEvaluationFunction(),
                        piR1 = r1.getPi(), piR2 = r2.getPi();
                
                if(eFr1 > eFr2) return -1;
                
                if(eFr1 < eFr2) return 1;
                
                if(piR1 > piR2) return -1;
                
                if(piR1 < piR2) return 1;
                
                return 0;
            }
        };
    }
}
