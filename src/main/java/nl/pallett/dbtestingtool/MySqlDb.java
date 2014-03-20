/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 *
 * @author Dennis
 */
public class MySqlDb extends Database {
    protected Connection conn;
    
    @Override
    public String toString () {
        return "MySQL";
    }

    @Override
    public void openConnection() throws SQLException {
        conn = this.openStandardConnection("com.mysql.jdbc.Driver", "jdbc:mysql");
        
        // turn on query history logging
        // needed to fetch query execution time later on
        Statement q = conn.createStatement();
        q.execute("SET profiling = 1;");
    }

    @Override
    public void closeConnection() throws SQLException {
        if (conn != null && conn.isClosed() == false) {
            conn.close();
        }
    }

    @Override
    public boolean tableExists() throws SQLException {
        return this.standardTableExists(conn);
    }

    @Override
    public QueryResult runQuery(String sql) throws SQLException {
        Statement q = conn.createStatement();
       
        System.out.println(sql);
        ResultSet res = q.executeQuery(sql);
        
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
        res = newQ.executeQuery("SHOW PROFILE");
        
        double runTime = 0;
        
        while(res.next()) {
            runTime += res.getDouble("Duration");
        }
            
        newQ.close();
        
        // convert from sec to msec
        runTime = runTime * 1000;
        
        QueryResult queryResult = new QueryResult();
        queryResult.setRecordCount(recordCount);
        queryResult.setRunTime(runTime);
        
        return queryResult;
    }
    
}
