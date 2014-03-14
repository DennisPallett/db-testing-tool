/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.sql.*;
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
    
    public abstract void openConnection () throws SQLException;
    
    public abstract void closeConnection () throws SQLException;
    
    public abstract boolean tableExists() throws SQLException;
    
    public abstract QueryResult runQuery(String sql) throws SQLException;
    
}
