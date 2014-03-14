/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

/**
 *
 * @author Dennis
 */
public class QueryResult {
    protected int recordCount;
    
    protected double runTime;

    /**
     * @return the recordCount
     */
    public int getRecordCount() {
        return recordCount;
    }

    /**
     * @param recordCount the recordCount to set
     */
    public void setRecordCount(int recordCount) {
        this.recordCount = recordCount;
    }

    /**
     * @return the runTime
     */
    public double getRunTime() {
        return runTime;
    }

    /**
     * @param runTime the runTime to set
     */
    public void setRunTime(double runTime) {
        this.runTime = runTime;
    }
    
    
    
}
