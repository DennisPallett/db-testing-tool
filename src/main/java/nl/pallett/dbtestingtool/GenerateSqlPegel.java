/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.apache.commons.lang.RandomStringUtils;

/**
 *
 * @author Dennis
 */
public class GenerateSqlPegel extends GenerateSql {
    
    

    @Override
    protected String generatePostgres(Node queryNode, int groupCount, int rowCount) throws Exception {
        String[] extract = extractStartEnd(queryNode);        
        String start = extract[0];
        String end = extract[1];
        
        String sql = " SELECT MEDIAN(PEGEL) FROM " + table;
        sql += " WHERE timed >= " + start + " AND timed <= " + end;
        
        if (groupCount > 1) {
            String groupBy = generateGroupBy(start, end, groupCount, rowCount);
            sql += " GROUP BY " + groupBy;       
        }        
        
        return sql;
    }

    @Override
    protected String generateMonetdb(Node queryNode, int groupCount, int rowCount) throws Exception {
        String[] extract = extractStartEnd(queryNode);        
        String start = extract[0];
        String end = extract[1];
        
        String sql = " SELECT MEDIAN(PEGEL)";
        
        if (groupCount > 1) {
            String groupBy = generateGroupBy(start, end, groupCount, rowCount);
            sql += ", " + groupBy + " AS timedGroup";
        }        
        
        sql += " FROM " + table;
        sql += " WHERE timed >= " + start + " AND timed <= " + end;
        
        if (groupCount > 1) {
            sql += " GROUP BY timedGroup";
        }
        
        return sql;        
    }
    
    protected String generateGroupBy (String start, String end, int groupCount, int rowCount) {
        long startTime = Long.parseLong(start.substring(0, start.length()-3));
        long endTime = Long.parseLong(end.substring(0, end.length()-3));
        long range = endTime - startTime;
        long divideBy = range / groupCount;

        // this is necessary to get groupCount nr. of buckets:
        divideBy = divideBy + (divideBy/groupCount);
            
        return "(timed / " + divideBy + "000)";               
    }
    
    protected String[] extractStartEnd(Node queryNode) throws Exception {
        String[] ret = new String[2];
        
        NamedNodeMap attr = queryNode.getAttributes();
        Node startNode = attr.getNamedItem("start");
        Node endNode = attr.getNamedItem("end");
        
        if (startNode == null || endNode == null) {
            throw new Exception("Missing start or end attribute in query element!");
        }
        
        ret[0] = startNode.getTextContent();
        ret[1] = endNode.getTextContent();        
        
        return ret;
    }
    
}
