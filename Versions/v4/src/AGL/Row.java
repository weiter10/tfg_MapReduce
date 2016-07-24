/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *
 * @author manu
 */
public abstract class Row
{
    protected final Attribute[] row;//La última posición indica la clase a la
                                      //que pertenece
    protected Map<Integer, Attribute> indexAtr;
    protected Map<Integer, Integer> indexPos;
    
    public Row(Attribute[] row)
    {
        this.row = row;
        this.buildMaps();
    }
    
    
    public Row(Row paramRow)
    {
        this.row = new Attribute[paramRow.row.length];
        
        for (int i = 0; i < row.length; i++) row[i] = new Attribute(paramRow.row[i]);
        
        this.buildMaps();
    }
    
    
    private void buildMaps()
    {
        indexAtr = new HashMap();
        indexPos = new HashMap();
        int count = 0;
        int numAtr = 0;
        
        for (int i = 0; i < this.getSizeChromosome(); i++)
        {
            if(count < row[numAtr].size())
            {
                indexAtr.put(i, row[numAtr]);
                indexPos.put(i, count);
                count++;
            }
            else
            {
                count = 0;
                numAtr++;
                i--;
            }
        }
    }
    
    
    @Override
    public boolean equals(Object obj)
    {
        //System.out.println("Equals");
        
        if(obj == null) return false;
        
        else if (!(obj instanceof Row)) return false;
        
        Row rowParam = (Row)obj;
        
        for (int i = 0; i < row.length; i++)
        {
            if(!row[i].equals(rowParam.row[i])) return false;
        }
        
        return true;
    }

    @Override
    public int hashCode()
    {
        int hash = 5;
        hash = 67 * hash + Arrays.deepHashCode(this.row);
        hash = 67 * hash + Objects.hashCode(this.indexAtr);
        hash = 67 * hash + Objects.hashCode(this.indexPos);
        
        //System.out.println("hashCode: " + hash);
        
        return hash;
    }
    
    
    public int getNumAtr()
    {
        return this.row.length;
    }
    
    /**
     * Devuelve el patrón del cromosoma
     * @return 
     */
    public int[] getPattern()
    {
        int[] pattern = new int[this.row.length];
        
        for (int i = 0; i < this.row.length; i++) pattern[i] = this.row[i].size();
        
        return pattern;
    }
    
    public Attribute getClassAttribute()
    {
        return new Attribute(row[row.length-1]);
    }
    
    
    public int getSizeChromosome()
    {
        int size = 0;
        
        for(Attribute atr : row) size += atr.size();
        
        return size;
    }
    
    
    public char getValue(int posi)
    {
        return this.indexAtr.get(posi).getValue(this.indexPos.get(posi));
    }
    
    /**
     * Devuelve el número de ceros sin tener en cuenta el atributo de clase
     * @return 
     */
    public int getNumZeros()
    {
        int zeros = 0;
        
        for (int i = 0; i < this.row.length-1; i++)
            zeros += row[i].getNumZeros();
        
        return zeros;
    }
    
    
    @Override
    public String toString()
    {
        String result = "";
        
        for(Attribute atr : this.row) result += atr.toString() + ",";
        
        return result;
    }
}
