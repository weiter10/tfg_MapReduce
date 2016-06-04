/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;

import java.util.Arrays;

/**
 *
 * @author manu
 */
public class Attribute
{
    char[] tab;
    
    public Attribute(char[] atr)
    {
        this.tab = atr;
    }
    
    public Attribute(Attribute atrParam)
    {
        this.tab = atrParam.tab.clone();
    }

    @Override
    public int hashCode()
    {
        int hash = 3;
        hash = 89 * hash + Arrays.hashCode(this.tab);
        return hash;
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if(obj == null) return false;
        
        else if (!(obj instanceof Attribute)) return false;
        
        Attribute atr = (Attribute) obj;
        
        for (int i = 0; i < tab.length; i++)
        {
            if(tab[i] != atr.tab[i]) return false;
        }
        
        return true;
    }
    
    public int size()
    {
        return this.tab.length;
    }
    
    
    /**
     * Devuelve true si "this" cubre al atributo "atr"
     * @param atr
     * @return 
     */
    public boolean coverAttribute(Attribute atr)
    {
        for (int i = 0; i < this.tab.length; i++)
        {
            if(atr.tab[i] == '1' && atr.tab[i] != tab[i]) return false;
        }
        
        return true;
    }
    
    
    /**
     * Devuelve true si this y paramAtr son la misma clase
     * @param paramAtr
     * @return 
     */
    public boolean sameClass(Attribute paramAtr)
    {
        return this.equals(paramAtr);
    }
    
    
    /**
     * Modifica this para que cubra al atributo atr
     * @param atr
     */
    public void modifyToCover(Attribute atr)
    {
        for (int i = 0; i < this.tab.length; i++)
        {
            if(atr.tab[i] == '1' && atr.tab[i] != tab[i]) tab[i] = '1';
        }
    }
    
    
    public char getValue(int posi)
    {
        return this.tab[posi];
    }
    
    
    public void setValue(int posi, char value)
    {
        this.tab[posi] = value;
    }
    
    
    public int getNumZeros()
    {
        int zeros = 0;
        
        for(char c : tab) if(c == '0') zeros++;
        
        return zeros;
    }
    
    
    @Override
    public String toString()
    {
        String result = "";
        
        for(char c : this.tab)
            result += c;
        
        return result;
    }
}
