/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public abstract class GenerateSql {
    protected String table;
    
    protected abstract String generatePostgres (Node queryNode, int groupCount, int rowCount) throws Exception;
    protected abstract String generateMonetdb (Node queryNode, int groupCount, int rowCount) throws Exception;
    protected abstract String generateSqlServer (Node queryNode, int groupCount, int rowCount) throws Exception;
    
    public String generateSQL (Database database, Node queryNode, int groupCount, int rowCount) throws Exception {
        this.table = database.getTable();
        
        String sql = "";
        if (database instanceof PostgresDb) {
            sql = generatePostgres(queryNode, groupCount, rowCount);
        } else if (database instanceof MonetDb) {
            sql = generateMonetdb(queryNode, groupCount, rowCount);
        } else if (database instanceof SqlServerDb) {
            sql = generateSqlServer(queryNode, groupCount, rowCount);
        } else {
            System.err.println("ERROR: unknown database!");
        }
        
        return sql;
    }
    
}
