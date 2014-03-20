/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author Dennis
 */
public class MonetDb extends Database {
    protected Connection conn;
    
    /**
    * Put escaped quotes around a value.
    */
    public static String quoteValue(String value) {
        return "'" + value.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'";
    }
    
    @Override
    public String toString () {
        return "MonetDB";
    }

    @Override
    public void openConnection() throws SQLException {
        conn = this.openStandardConnection("nl.cwi.monetdb.jdbc.MonetDriver", "jdbc:monetdb");
        
        // turn on query history logging
        // needed to fetch query execution time later on
        Statement q = conn.createStatement();
        q.execute("CALL querylog_enable();");
    }

    @Override
    public void closeConnection() throws SQLException {
        if (conn != null && conn.isClosed() == false) {
            Statement q = conn.createStatement();
            
            q.execute("CALL querylog_disable();");
            q.execute("CALL querylog_empty();");
            
            conn.close();
        }
    }

    @Override
    public boolean tableExists() throws SQLException {
        return this.standardTableExists(conn);
    }

    @Override
    public QueryResult runQuery(String sql) throws SQLException {
        PreparedStatement q = conn.prepareStatement(sql);
       
        System.out.println(sql);
        ResultSet res = q.executeQuery();
        
        // count number of rows returned
        int recordCount = 0;
        while(res.next()) {
            recordCount++;
        }
        
        res.close();
        q.close();        
     
        // fetch run time from db
        // this assumes no other queries have been run in the mean time
        // hence, not thread-safe or safe from other clients
        Statement newQ = conn.createStatement();
        String timeQuery =  " SELECT query, run" +
                            " FROM sys.querylog_calls AS qc" +
                            " INNER JOIN sys.querylog_catalog AS qd ON qc.id = qd.id" +
                            " WHERE LCase(query) LIKE " + MonetDb.quoteValue("prepare " + sql.toLowerCase() + "%") +
                            " ORDER BY \"start\" DESC" +
                            " LIMIT 1";
        System.out.println(timeQuery);
        res = newQ.executeQuery(timeQuery);
        
        if (res.next() == false) {
            throw new SQLException("Unable to retrieve query history");
        }
        
        // retrieve run time in usec
        double runTime = res.getInt("run");
        
        // convert from usec to msec
        runTime = runTime / 1000;
        
        QueryResult queryResult = new QueryResult();
        queryResult.setRecordCount(recordCount);
        queryResult.setRunTime(runTime);
        
        return queryResult;
    }
    
}
