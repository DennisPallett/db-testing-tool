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
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException("PostgreSQL JDBC Driver cannot be initialized");
        }
        
        String url = "jdbc:postgresql://" + this.host + "/" + this.name;
        Properties props = new Properties();
        
        if (user.length() > 0) {
            props.setProperty("user", user);
            props.setProperty("password", password);
        }

        conn = DriverManager.getConnection(url, props);
    }

    @Override
    public void closeConnection() throws SQLException {
        if (conn != null && conn.isClosed() == false) {
            conn.close();
        }
    }

    @Override
    public boolean tableExists() throws SQLException {
        PreparedStatement q = conn.prepareStatement("SELECT * FROM " + this.table + " LIMIT 1");
        
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
