/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package MapReduce;

import AGL.Rule;

/**
 *
 * @author manu
 */
public class infoRule
{
    private final String classRule;
    private int positives, negatives, count;
    private double pi;
    
    
    public infoRule(String c)
    {
        this.classRule = c;
        this.positives = 0;
        this.negatives = 0;
        count = 0;
        pi = 0;
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
    
    public void addPi(double p)
    {
        pi += p;
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
    
    
}
