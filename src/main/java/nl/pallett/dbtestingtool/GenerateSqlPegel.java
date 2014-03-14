/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 * @author Dennis
 */
public class GenerateSqlPegel extends GenerateSql {

    @Override
    protected String generatePostgres(Node queryNode, int groupCount, int rowCount) throws Exception {
        String sql = " SELECT COUNT(*) FROM " + table;
        
        NamedNodeMap attr = queryNode.getAttributes();
        Node startNode = attr.getNamedItem("start");
        Node endNode = attr.getNamedItem("end");
        
        if (startNode == null || endNode == null) {
            throw new Exception("Missing start or end attribute in query element!");
        }
        
        String start = startNode.getTextContent();
        String end = endNode.getTextContent();
        
        sql += " WHERE timed >= " + start + " AND timed <= " + end;
        
        if (groupCount > 1) {
            int startTime = Integer.parseInt(start.substring(0, start.length()-3));
            int endTime = Integer.parseInt(end.substring(0, end.length()-3));
            int range = endTime - startTime;
            int divideBy = range / groupCount;
            
            sql += " GROUP BY timed / " + divideBy + "000";            
        }        
        
        return sql;
    }
    
}
