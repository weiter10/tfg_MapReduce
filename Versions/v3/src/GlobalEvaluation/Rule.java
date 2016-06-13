/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GlobalEvaluation;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author manu
 */
public final class Rule extends Row implements Comparable
{
    private double evaluationFunction = -1, pi = -1;
    private Set<Example> coveredPositives, coveredNegative;
    
    
    public Rule(Attribute[] row, Set<Example> examples)
    {
        super(row);
        this.coveredPositives = new HashSet();
        this.coveredNegative = new HashSet();
        this.calculatePerformance(examples);
    }

    
    /**
     * Actualiza la función de evaluación y el pi de la regla
     * @param examples
     */
    private void calculatePerformance(Set<Example> examples)
    {
        int value;
        double valueD;
        
        //Testeamos la regla con todos los ejemplos aún válidos
        for(Example example1 : examples)
        {
            value =  this.coverExample(example1);
            
            if(value == 1)
                this.coveredPositives.add(example1);
            
            else if(value == -1)
                this.coveredNegative.add(example1);
        }
        
        valueD = 1 + ((double)this.getNumZeros()/(this.getSizeChromosome()-this.getClassAttribute().size()));
        
        evaluationFunction = Math.pow(valueD, -this.coveredNegative.size());
        pi = this.coveredPositives.size()*evaluationFunction;
    }
    
    
    /**
     * Actualiza la función de evaluación y el pi de la regla
     * 
     * @param positivesCov
     */
    public void updatePerformance(Set<Example> positivesCov)
    {
        double valueD;
        
        this.coveredPositives.removeAll(positivesCov);
        
        valueD = 1 + ((double)this.getNumZeros()/(this.getSizeChromosome()-this.getClassAttribute().size()));
        
        evaluationFunction = Math.pow(valueD, -this.coveredNegative.size());
        pi = this.coveredPositives.size()*evaluationFunction;
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
        
        //En caso de tener el mismo pi, ordenado por la función de evaluación
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
    
    
    public String show()
    {
        return super.toString() + "\teF:" + this.evaluationFunction + 
                "\tpi:" + this.pi + "\tnegatives:" + this.coveredNegative.size() + 
                "\tpositives:" + this.coveredPositives.size();
    }
    
    /**
     * Devuelve el cuerpo de la regla
     * @return 
     */
    public String getStrBody()
    {
        String result = "";
        
        for(int i = 0; i < row.length-1; i++) result += row[i].toString() + ",";
        
        return result;
    }
    
    
    /**
     * Devuelve la clase de la regla
     * @return 
     */
    public String getStrClass()
    {
        return this.getClassAttribute().toString();
    }

    public int getPositives() {
        return this.coveredPositives.size();
    }

    public int getNegatives() {
        return this.coveredNegative.size();
    }

    public Set<Example> getCoveredPositives() {
        return coveredPositives;
    }

    public Set<Example> getCoveredNegative() {
        return coveredNegative;
    }
}
