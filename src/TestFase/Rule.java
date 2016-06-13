/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package TestFase;



/**
 *
 * @author manu
 */
public final class Rule extends Row
{
    public Rule(Attribute[] row)
    {
        super(row);
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
    
    
}
