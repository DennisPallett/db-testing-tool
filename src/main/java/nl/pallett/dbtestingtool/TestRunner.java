/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Observable;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public class TestRunner extends Observable implements Runnable {

    /**
     * @return the currTestSet
     */
    public TestSet getCurrTestSet() {
        return currTestSet;
    }

    /**
     * @return the currQuerySet
     */
    public QuerySet getCurrQuerySet() {
        return currQuerySet;
    }

    /**
     * @return the currQueryId
     */
    public int getCurrQueryId() {
        return currQueryId;
    }

    /**
     * @return the currGroup
     */
    public int getCurrGroup() {
        return currGroup;
    }
    public enum State {NEW, RUNNING, FAILED, FINISHED};
    
    protected ArrayList<TestSet> testSetList;
    
    protected ArrayList<Integer> groupList;
    
    protected Database database;
    
    protected int timeout;
    
    protected String resultsDir;
    
    protected int queryCountPerSet;
    
    protected State state;
    
    protected Exception lastException;
    
    protected TestSet currTestSet = null;
    
    protected QuerySet currQuerySet = null;
    
    protected int currQueryId = -1;
    
    protected int currGroup = -1;
    
    protected GenerateSql generateSql;
    
    protected int queriesFinished = 0;
    
    protected int querySetsFinished = 0;
        
    public TestRunner () {
        state = State.NEW;
    }
    
    @Override
    public void run() {
        state = State.RUNNING;
        logMessage("Testrunner started");
        
        // check if there are TestSets
        if (testSetList.isEmpty()) {
            logMessage("There are no TestSets!");
            state = State.FINISHED;
            return;
        }
        
        // determine which dataset is being used
        TestSet testSet = testSetList.get(0);
        String source = testSet.getSource().toLowerCase();
        
        if (source.equals("pegel_andelfingen")) {
            generateSql = new GenerateSqlPegel();
        } else {
            logMessage("ERROR: unknown dataset '" + source + '"');
            state = State.FAILED;
            return;
        }
        
        logMessage("Using dataset " + source);
        
        logMessage("Opening connection to database...");
        try {
            database.openConnection();
        } catch (SQLException e) {
            state = State.FAILED;
            lastException = e;
            logMessage("ERROR: unable to establish database connection");
            return;
        }
        logMessage("Connection opened");
        
        try {
            runQueries();
        } catch (Exception e) {
            state = State.FAILED;
            lastException = e;
            logMessage("FATAL ERROR during execution of queries");
            return;
        }
        
        logMessage("Closing connection...");
        try {
            database.closeConnection();
        } catch (SQLException ex) {
            lastException = ex;
            logMessage("Unable to close connection");
        }
        logMessage("Connection closed");
        
        state = State.FINISHED;
        logMessage("Testrunner finished");        
    }
    
    protected void runQueries() throws SQLException {
        for(TestSet testSet : testSetList) {
            currTestSet = testSet;
            
            ArrayList<QuerySet> querySetList = testSet.getQuerySetList();
            for(QuerySet querySet : querySetList) {
                currQuerySet = querySet;
                
                ArrayList<Node> queryList = querySet.getQueryNodes();
                for(int i=0; i < queryCountPerSet; i++) {
                    Node queryNode = queryList.get(i);
                    currQueryId = i;
                    
                    for(Integer group : groupList) {
                        currGroup = group;
                        runQuery(queryNode, group, querySet.getSize());
                    }
                }
                
                querySetsFinished++;
            }
        }       
    }
    
    protected void runQuery(Node queryNode, Integer group, int rowCount) throws SQLException {
        // generate SQL from this query node
        String sql;
        try {
            sql = generateSql.generateSQL(database, queryNode, group, rowCount);
        } catch (Exception ex) {
            throw new SQLException(ex);
        }
        
        logMessage("Executing query:\n" + sql);
        
        QueryResult queryResult = database.runQuery(sql);
        
        if (queryResult.getRecordCount() != group.intValue() && queryResult.getRecordCount() != group.intValue()+1) {
            throw new SQLException("Record count of result does not match number of expected groups. "
                    + "This is an indication that the query is incorrect! Expected: " + group + ", got: " + queryResult.getRecordCount());
        }       
        
        logMessage("Query has finished, total records returned: " + queryResult.getRecordCount());
        logMessage("Query runtime: " + queryResult.getRunTime() + " ms");
        
        // TODO: save results        
        
        queriesFinished++;        
    }
    
    protected void logMessage(String msg) {
        setChanged();
        notifyObservers(msg);
    }
    
    public int getQueriesFinished () {
        return queriesFinished;
    }
    
    public int getQuerySetsFinished () {
        return querySetsFinished;
    }
    
    public Exception getLastException () {
        return this.lastException;
    }
    
    public State getState () {
        return this.state;
    }
    
    /**
     * Returns the number of sets
     * @return number of sets
     */
    public int getSetCount () {
        int count = 0;
        for(TestSet querySet : testSetList) {
            count += querySet.getSetCount();
        }
        return count;
    }
    
    /**
     * Returns the total number of queries of this test
     * @return number of queries
     */
    public int getQueryCount () {       
        return getSetCount() * getGroupList().size() * getQueryCountPerSet();
    }

    /**
     * @return the testSetList
     */
    public ArrayList<TestSet> getTestSetList() {
        return testSetList;
    }

    /**
     * @param testSetList the testSetList to set
     */
    public void setTestSetList(ArrayList<TestSet> querySetList) {
        this.testSetList = querySetList;
    }

    /**
     * @return the groupList
     */
    public ArrayList<Integer> getGroupList() {
        return groupList;
    }

    /**
     * @param groupList the groupList to set
     */
    public void setGroupList(ArrayList<Integer> groupList) {
        this.groupList = groupList;
    }

    /**
     * @return the database
     */
    public Database getDatabase() {
        return database;
    }

    /**
     * @param database the database to set
     */
    public void setDatabase(Database database) {
        this.database = database;
    }

    /**
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout the timeout to set
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * @return the resultsDir
     */
    public String getResultsDir() {
        return resultsDir;
    }

    /**
     * @param resultsDir the resultsDir to set
     */
    public void setResultsDir(String resultsDir) {
        this.resultsDir = resultsDir;
    }

    /**
     * @return the queryCountPerSet
     */
    public int getQueryCountPerSet() {
        return queryCountPerSet;
    }

    /**
     * @param queryCount the queryCountPerSet to set
     */
    public void setQueryCountPerSet(int queryCount) {
        this.queryCountPerSet = queryCount;
    }
    
}
