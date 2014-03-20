/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.*;
import java.util.Properties;
import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public abstract class Database {
    
    protected String user;
    
    protected String password;
    
    protected String name;
    
    protected String host;
    
    protected String table;

    /**
     * @return the user
     */
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @param host the host to set
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * @return the table
     */
    public String getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(String table) {
        this.table = table;
    }
    
    protected Connection openStandardConnection (String driverName, String urlPrefix) throws SQLException {
        // load driver class
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException ex) {
            throw new SQLException(this.toString() + " JDBC Driver cannot be initialized");
        }
                
        String url = urlPrefix + "://" + this.host + "/" + this.name;
        Properties props = new Properties();
        
        if (user.length() > 0) {
            props.setProperty("user", user);
            props.setProperty("password", password);
        }

        return DriverManager.getConnection(url, props);
    }
    
    public abstract void openConnection () throws SQLException;
    
    public abstract void closeConnection () throws SQLException;
    
    protected boolean standardTableExists (Connection conn) throws SQLException {
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
    
    public abstract boolean tableExists() throws SQLException;
    
    public abstract QueryResult runQuery(String sql) throws SQLException;
    
}
