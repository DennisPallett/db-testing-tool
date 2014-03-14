/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.util.ArrayList;
import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public class QuerySet {
    protected int id;
    
    protected String name;
    
    protected int size;
    
    protected ArrayList<Node> queryNodes;
    
    public QuerySet (String name, int size, ArrayList<Node> queryNodes) {
        this.name = name;
        this.size = size;
        this.queryNodes = queryNodes;
    }
    
    public ArrayList<Node> getQueryNodes () {
        return this.queryNodes;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the size
     */
    public int getSize() {
        return size;
    }
    
    
    
}
