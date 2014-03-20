/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public class PostgresDb extends Database {
    protected Connection conn;
       
    @Override
    public String toString () {
        return "PostgreSQL";
    }

    @Override
    public void openConnection() throws SQLException {
        conn = this.openStandardConnection("org.postgresql.Driver", "jdbc:postgresql");
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
        
        // use EXPLAIN ANALYZE to let MonetDB time query
        sql = "EXPLAIN ANALYZE " + sql;
        
        ResultSet res = q.executeQuery(sql);
        
        ArrayList<String> info = new ArrayList<String>();
        while(res.next()) {
            info.add(res.getString(1));
        }
        
        res.close();
        q.close();
        
        // analyze info
        String rowString = info.get(0);
        int start = rowString.lastIndexOf("rows=") + 5;
        int end = rowString.indexOf(' ', start);
        
        int recordCount = Integer.parseInt(rowString.substring(start, end));
        
        String runTimeString = info.get(info.size()-1).toLowerCase();
        start = runTimeString.indexOf("runtime: ") + "runtime: ".length();
        end = runTimeString.indexOf(" ms", start);
        
        double runTime = Double.parseDouble(runTimeString.substring(start, end));
        
        QueryResult queryResult = new QueryResult();
        queryResult.setRecordCount(recordCount);
        queryResult.setRunTime(runTime);
        
        return queryResult;
    }
    
}
