/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Job_Training;



/**
 *
 * @author manu
 */
public class InfoRule
{
    private final String classRule;
    private int positives, negatives, count, numZeros, sizeChromosome;
    private double pi;
    
    
    public InfoRule(String cl, String rule)
    {
        this.classRule = cl;
        this.positives = 0;
        this.negatives = 0;
        this.numZeros = 0;
        this.sizeChromosome = rule.length();
        count = 0;
        pi = 0;
        char[] pal = rule.toCharArray();
        
        for(char c : pal)
            if(c == '0')
                this.numZeros++;
    }
    
    public void addPositives(int p)
    {
        this.positives += p;
    }
    
    public void addNegatives(int n)
    {
        this.negatives += n;
    }
    
    public void increaseCount()
    {
        count++;
    }

    public int getPositives() {
        return positives;
    }

    public int getNegatives() {
        return negatives;
    }

    public String getClassRule() {
        return classRule;
    }

    public int getCount() {
        return count;
    }

    public double getPi() {
        return pi;
    }
    
    public void calculatePi()
    {
        double valueD = 1 + ((double)this.numZeros/sizeChromosome);
        double evaluationFunction = Math.pow(valueD, -negatives);
        pi = positives*evaluationFunction*count;
    }
}
