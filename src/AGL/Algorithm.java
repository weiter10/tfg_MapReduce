/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package AGL;
import Job_Training.MapTraining;
import Job_Training.Training;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;

/**
 *
 * @author manu
 */
public class Algorithm
{
    public static int sizePopulation = 30, limit = 200;
    private final Parse dataset;
    private Set<Rule> population;
    private final Random rnd;
    private final Set<Rule> finalRules;
    public MapTraining job;
    
    
    public Algorithm(String dataString, MapTraining job) throws FileNotFoundException, IOException
    {
        this.job = job;
        dataset = new Parse(dataString, job);
        this.rnd = new Random();
        this.rnd.setSeed(dataset.getSeedRandomNumbers());
        this.finalRules = new HashSet();
    }
    
    
    public Set<Rule> run()
    {
        this.population = this.generateInitialPopulation();
        
        //Local variables
        int numVoters = (int) (sizePopulation*0.9);
        ArrayList<Rule> parents;
        Set<Integer> validExamples = this.dataset.getValidExamples();
        int numIterationsWithOutImprove = 0, countFail = 0, 
                countGood = 0, vF = 0, vG = 0;
        double lastMeanEvaluation = this.getMeanEvaluationFunction(),
                actualMeanEvaluation, pCrossing = 0.6;
        Rule[] rules, coupleParents = new Rule[2];
        Rule best;
        PriorityQueue<Rule> populationOrdered = new PriorityQueue(Collections.reverseOrder()),
                orderedByEF = new PriorityQueue(Rule.comparatorByEF());
        long start = System.currentTimeMillis(), elapsedTimeMillis;
        //--
        
        while(validExamples.size() > 1)
        {
            elapsedTimeMillis = System.currentTimeMillis()-start;
            //Si ha pasado mas de 400s informamos al context que seguimos trabajando
            //para evitar que cancele el MAP
            if(elapsedTimeMillis/1000F > 400)
            {
                job.continueWorking();
                start = System.currentTimeMillis();
            }
            
            //Ojo, en el código original está puesto el valor del clasificador
            if(numIterationsWithOutImprove < limit)
            {
                parents = this.universalSuffrage();
                
                if(numVoters > validExamples.size()) numVoters = validExamples.size();
                if(numVoters%2 != 0) numVoters--;//Obligamos a que los padres sean pares
                Set<Rule> sR = new HashSet(parents);
                
                if(sR.size() != numVoters)
                {
                    vF++;
                    System.out.println("Resultado del US: " + sR.size() + " en lugar de " + numVoters);
                }
                
                else vG++;

                for(int posi = 0; posi < parents.size(); posi += 2)
                {
                    if(rnd.nextDouble() < pCrossing)
                    {
                        coupleParents[0] = parents.get(posi);
                        coupleParents[1] = parents.get(posi+1);
                        rules = this.crossing(coupleParents[0], coupleParents[1]);

                        //Aplicamos la mutación
                        for (Rule rule : rules) this.mutate(rule);

                        //Añadimos los padres y los hijos, ordenandolos 
                        for (int i = 0; i < 2; i++)
                        {
                            /*
                            Eliminamos los padres de la población. Como tenemos orderedByEF
                            con los padres y los hijos, posteriormente solo tenemos que 
                            añadir los dos individuos mejores
                            */
                            this.population.remove(coupleParents[i]);
                            orderedByEF.add(coupleParents[i]);
                            //Añadimos el hijo solo si no es un cromosoma trivial,
                            //todo "0" o todo "1"
                            if(rules[i].validChromosome()) orderedByEF.add(rules[i]);
                        }

                        sR = new HashSet(orderedByEF);
                        if(sR.size() != 4)
                        {
                            System.out.println("Resultado de la reproduccion: " + sR.size() + 
                                    " en lugar de 4 con " + validExamples.size() + " ejemplos validos");
                            countFail++;
                        }
                        else countGood++;

                        //Añadimos los dos mejores
                        for (int i = 0; i < 2; i++)
                        {
                            int size = this.population.size();

                            while(this.population.size() == size && !orderedByEF.isEmpty())
                            {
                                this.population.add(orderedByEF.poll());
                            }
                        }

                        orderedByEF.clear();

                        actualMeanEvaluation = this.getMeanEvaluationFunction();

                        //Si la media de la función de evaluación de la población ha
                        //mejorado
                        if(actualMeanEvaluation > lastMeanEvaluation)
                        {
                            lastMeanEvaluation = actualMeanEvaluation;
                            numIterationsWithOutImprove = 0;
                        }

                        else numIterationsWithOutImprove++;
                    }
                }
                
                //TODO poner una política en condiciones para reducir la población
                //si aumenta del máximo
                while(this.population.size() > this.sizePopulation)
                    this.population.remove(this.population.iterator().next());
            }
            //Reducción de ejemplos, implica reevaluar todas las reglas de la
            //población
            else
            {
                populationOrdered.addAll(this.population);
                best = populationOrdered.element();//Seleccionamos la regla con mejor pi
                
                //Si es el primer enfriamiento añadimos todas las reglas
                if(finalRules.isEmpty())
                {
                    for(Rule rule : this.population) finalRules.add(new Rule(rule));
                }
                //Sino solo la mejor
                else finalRules.add(new Rule(best));
                
                //Eliminamos la regla de la población
                this.population.remove(best);
                //Eliminamos los ejemplos que cubre la regla
                this.dataset.removeCoverExamples(best);
                //Añadimos una regla aleatoria para sustituir la eliminada
                int size = this.population.size();
                
                while(size == this.population.size())
                    this.population.add(Rule.generateRandomRule(rnd, best.getPattern(), dataset));
                
                //Actualizamos el performance de todas las reglas
                for(Rule rule : this.population) rule.updatePerformance();
                
                numIterationsWithOutImprove = 0;
                populationOrdered.clear();
            }
            
            //Error si el tamaño de la población es distinto que el configurado
            if(this.population.size() != this.sizePopulation)
            {
                System.err.println("Size: " + this.population.size());
            }
        }
        
        //Si nos queda un ejemplo por cubrir, añadimos una regla que lo cubre al
        //conjunto de reglas finales
        if(validExamples.size() == 1)
        {
            Rule r = this.sownOperator(this.dataset.getData().get(validExamples.iterator().next()));
            this.finalRules.add(r);
        }
        
        dataset.resetValidExamples();
        
        for(Rule r : this.finalRules) r.updatePerformance();
        
        
        System.out.println("countFail: " + countFail);
        System.out.println("countGood: " + countGood);
        System.out.println("votersFail: " + vF);
        System.out.println("votersGood: " + vG);
        
        return this.finalRules;
    }

    
    /**
     * Método que cruza dos padres, aplicado según lo explicado en la página 68
     * y 69 de la tesis
     * @param parent1
     * @param parent2
     * @return 
     */
    private Rule[] crossing(Rule parent1, Rule parent2)
    {
        double value = (parent1.getEvaluationFunction() + parent2.getEvaluationFunction())/2;
        
        if(value > this.getMeanEvaluationFunction())
        {
            return this.twoPointsCrossing(parent1, parent2);
        }
        
        else return this.uniformCrossing(parent1, parent2);
    }
    
    
    /**
     * Operador de sufragio universal. Se seleccionan las reglas que participarán 
     * en la reproducción
     * @return
     */
    private ArrayList<Rule> universalSuffrage()
    {
        int numVoters = (int) (sizePopulation*0.9);
        int posi, getMoreExamples = 0;
        Set<Integer> voters = new HashSet();
        ArrayList<Rule> outputRules = new ArrayList();
        ArrayList<Example> data = dataset.getData();
        ArrayList<Integer> validExamples = new ArrayList(dataset.getValidExamples());
        ArrayList<ArrayList<Integer>> validExamplesClasses = dataset.getValidExamplesByClasses();
        PriorityQueue<Rule> ruleOrder = new PriorityQueue(Collections.reverseOrder());//Ordenamos las reglas de mayor bondad a menor;
        Rule[] tabRoulette;
        double sum, acc, doubleD;
        double[] roulette;
        
        if(numVoters > validExamples.size()) numVoters = validExamples.size();
        
        if(numVoters%2 != 0) numVoters--;//Obligamos a que los padres sean pares
        
        //Comprobamos si alguna clase tiene menos ejemplos de los necesarios, si es así
        //cogemos el resto de ejemplo de la otra clase
        for(ArrayList<Integer> a1 : validExamplesClasses)
        {
            int numVotersClass = numVoters/2;
            
            if(numVotersClass > a1.size()) getMoreExamples = numVotersClass - a1.size();
        }
        
        //Cogemos los ejemplos de cada clase
        for(ArrayList<Integer> a1 : validExamplesClasses)
        {
            int numVotersClass = numVoters/2;
            ArrayList<Integer> validExamplesClass = new ArrayList(a1);
            
            if(numVotersClass > validExamplesClass.size()) numVotersClass = validExamplesClass.size();
            
            else numVotersClass += getMoreExamples;
            
            //Seleccionamos los índices de los ejemplos votantes
            for (int i = 0; i < numVotersClass; i++)
            {
                posi = rnd.nextInt(validExamplesClass.size());
                voters.add(validExamplesClass.get(posi));
                validExamplesClass.remove(posi);
            }
        }
        
        //Realizamos el sufragio
        for(int indexVoter : voters)
        {
            ruleOrder.clear();
            sum = 0;
            
            for(Rule rule1 : population)
            {
                //Si cubre el ejemplo
                if(rule1.coverExample(data.get(indexVoter)) == 1)
                {
                    ruleOrder.add(rule1);
                    sum += rule1.getPi();
                }
            }
            
            //Si la ruleta no está vacía
            if(!ruleOrder.isEmpty())
            {
                //Construimos la ruleta
                roulette = new double[ruleOrder.size()];
                acc = 0;
                tabRoulette = new Rule[ruleOrder.size()];

                for (int i = 0; i < roulette.length; i++)
                {
                    Rule rule1 = ruleOrder.poll();
                    acc = (rule1.getPi()/sum)+acc;
                    tabRoulette[i] = rule1;
                    roulette[i] = acc;
                }
                //--

                //Tiramos el dado
                doubleD = rnd.nextDouble();
                posi = 0;
                
                while(doubleD > roulette[posi]) posi++;
                
                //Agregamos la regla ganadora
                int size = outputRules.size();
                outputRules.add(tabRoulette[posi]);
                
                /*
                //Si la regla añadida es la misma que anteriormente, se añade
                //una aleatoria
                if(size == outputRules.size())
                {
                    //Como no se permiten reglas repetidas, puede que la que se añade ya exista,
                    //por tanto obligamos a la generación de una regla no repetida
                    while(size == outputRules.size()) outputRules.add(this.sownOperator(data.get(indexVoter)));
                }
                */
            }
            else
            {
                /*
                //Hacer sembrado con el ejemplo que no es cubierto por ninguna
                //regla
                int size = outputRules.size();
                
                //Como no se permiten reglas repetidas, puede que la que se añade ya exista,
                //por tanto obligamos a la generación de una regla no repetida
                while(size == outputRules.size()) outputRules.add(this.sownOperator(data.get(indexVoter)));
                */
                
                outputRules.add(this.sownOperator(data.get(indexVoter)));
            }
        }
        
        return outputRules;
    }
    
    
    /**
     * Operador de sembrado
     * @param ex
     * @return 
     */
    private Rule sownOperator(Example ex)
    {
        int[] pattern = ex.getPattern();
        Rule rule;
                
        rule = Rule.generateRandomRule(rnd, pattern, new Attribute(ex.getClassAttribute()), this.dataset);
        
        //Comprobar la regla con el ejemplo, y modificar la regla si procede.
        rule.modifyToCover(ex);
        
        return rule;
    }
    
    
    /**
     * Generación de la población inicial
     * @return 
     */
    private Set<Rule> generateInitialPopulation()
    {
        ArrayList<Example> data = dataset.getData();
        ArrayList<Integer> validExamples = new ArrayList(dataset.getValidExamples());
        Set<Rule> initialPopulation = new HashSet();
        int posi;
        
        //Creamos la población inicial
        for (int i = 0; i < this.sizePopulation; i++)
        {
            posi = rnd.nextInt(validExamples.size());
            int size = initialPopulation.size();
            
            while(initialPopulation.size() == size)
                initialPopulation.add(this.sownOperator(data.get(validExamples.get(posi))));
            
            validExamples.remove(posi);
        }
        
        return initialPopulation;
    }
    
    
    /**
     * Operador de cruce en dos puntos
     * @param parent1
     * @param parent2
     * @return 
     */
    private Rule[] twoPointsCrossing(Rule parent1, Rule parent2)
    {
        Rule[] rules = new Rule[2];
        rules[0] = new Rule(parent1);
        rules[1] = new Rule(parent2);
        //No queremos que se trocee el atributo de clase, por lo tanto le quitamos
        //la parte de la clase
        int sizeChromosome = parent1.getSizeChromosome()-parent1.getClassAttribute().size();
        int firstPoint = rnd.nextInt(sizeChromosome);
        int secondPoint = rnd.nextInt(sizeChromosome);

        if (firstPoint > secondPoint)
        {
            int temp = firstPoint;
            firstPoint = secondPoint;
            secondPoint = temp;
        }

        for (int i = 0; i < firstPoint; i++)
        {
            rules[0].setValue(i, parent1.getValue(i));
            rules[1].setValue(i, parent2.getValue(i));
        }
        
        for (int i = firstPoint; i <= secondPoint; i++)
        {
            rules[0].setValue(i, parent2.getValue(i));
            rules[1].setValue(i, parent1.getValue(i));
        }
        
        for (int i = secondPoint + 1; i < parent1.getSizeChromosome(); i++)
        {
            rules[0].setValue(i, parent1.getValue(i));
            rules[1].setValue(i, parent2.getValue(i));
        }
        
        rules[0].updatePerformance();
        rules[1].updatePerformance();
        
        return rules;
    }
    
    
    /**
     * Cruce Uniforme, A este método se le pasa dos reglas padre1 y padre2, y
     * genera dos reglas hijas, donde cada bit tiene una probalilidad del 50% de
     * pertenecer a una regla padre1 o a la padre2.
     *
     */
    private Rule[] uniformCrossing(Rule parent1, Rule parent2)
    {
        int sizeChromosome = parent1.getSizeChromosome()-parent1.getClassAttribute().size();
        Rule[] rules = new Rule[2];
        rules[0] = new Rule(parent1);
        rules[1] = new Rule(parent2);
        
        for (int i = 0; i < sizeChromosome; i++)
        {
            if (rnd.nextDouble() < 0.5)
            {
                rules[0].setValue(i, parent1.getValue(i));
                rules[1].setValue(i, parent2.getValue(i));
            }
            else
            {
                rules[0].setValue(i, parent2.getValue(i));
                rules[1].setValue(i, parent1.getValue(i));
            }
        }
        
        if(rnd.nextDouble() > 0.5)
        {
            rules[0].setClassAttribute(parent2.getClassAttribute());
            rules[1].setClassAttribute(parent1.getClassAttribute());
        }
        
        rules[0].updatePerformance();
        rules[1].updatePerformance();
        
        return rules;
    }
    
    /**
     * Operador de mutación, se sigue el esquema indicado en la memoria de la
     * tesis
     * @param rule 
     */
    private void mutate(Rule rule)
    {
        double mutationP = 0.001, evaluationFunctionRule = rule.getEvaluationFunction(),
                meanEvaluationFuncton = this.getMeanEvaluationFunction();
        int sizeChromosome = rule.getSizeChromosome()-rule.getClassAttribute().size();
        
        for (int posiToMutate = 0; posiToMutate < rule.getSizeChromosome()-rule.getClassAttribute().size(); posiToMutate++)
        {
            if(rnd.nextDouble() <= mutationP)
            {
                //Se favorece la generelización
                if(evaluationFunctionRule > meanEvaluationFuncton)
                {
                    if(rnd.nextDouble() <= 0.8) rule.setValue(posiToMutate, '1');

                    else rule.setValue(posiToMutate, '0');
                }
                //Se favorece la especialización
                else
                {
                    if(rnd.nextDouble() <= 0.8) rule.setValue(posiToMutate, '0');

                    else rule.setValue(posiToMutate, '1');
                }
            }
        }
        
        rule.updatePerformance();
    }
    
    /**
     * Obtiene la media de la función de evaluación de la población
     * @return 
     */
    private double getMeanEvaluationFunction()
    {
        double acc = 0;
        
        for(Rule r : this.population) acc += r.getEvaluationFunction();
        
        return acc/this.population.size();
    }
}
