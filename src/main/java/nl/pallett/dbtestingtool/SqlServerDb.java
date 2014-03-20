/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

/**
 *
 * @author Dennis
 */
public class SqlServerDb extends Database {
    protected Connection conn;
    
    @Override
    public String toString () {
        return "SQL Server";
    }

    @Override
    public void openConnection() throws SQLException {
        conn = this.openStandardConnection("net.sourceforge.jtds.jdbc.Driver", "jdbc:jtds:sqlserver");
        
        // turn on time statistics
        Statement q = conn.createStatement();
        q.execute("SET STATISTICS TIME ON");
    }

    @Override
    public void closeConnection() throws SQLException {
        if (conn != null && conn.isClosed() == false) {
            conn.close();
        }
    }

    @Override
    public boolean tableExists() throws SQLException {
        PreparedStatement q = conn.prepareStatement("SELECT TOP 1 * FROM " + this.table);
        
        try {
            q.execute();
        } catch (SQLException e) {
            String msg = e.getMessage();
            if (e.getSQLState().equals("42P01")) {
                return false;
            } else {
                throw e;
            }
        }
        
        q.close();
        return true;
    }

    @Override
    public QueryResult runQuery(String sql) throws SQLException {
        Statement q = conn.createStatement();
        
        ResultSet res = q.executeQuery(sql);
        
       // count number of rows returned
        int recordCount = 0;
        while(res.next()) {
            recordCount++;
        }
        
        // loop through warnings to get execution time
        SQLWarning warning = q.getWarnings();
        double runTime = -1;
        while(warning != null) {
            String msg = warning.getMessage().toLowerCase();
            
            if (msg.indexOf("execution times") > -1) {
                int start = msg.indexOf("elapsed time = ") + "elapsed time = ".length();
                int end = msg.indexOf(" ms", start);
                
                runTime = Double.parseDouble(msg.substring(start, end));
                break;
            }
            
            warning = warning.getNextWarning();
        }
        
        res.close();
        q.close();
        
        if (runTime < 0) {
            throw new SQLException("Unable to retrieve run time for query");
        }
        
        QueryResult queryResult = new QueryResult();
        queryResult.setRecordCount(recordCount);
        queryResult.setRunTime(runTime);
        
        return queryResult;
    }
    
}
