/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Dennis
 */
public class GenerateSqlKnmi extends GenerateSql {
    protected final int STATION_COUNT = 10;

    @Override
    protected String generatePostgres(Node queryNode, int groupCount, int rowCount) throws Exception {
         String[] extract = extractStartEnd(queryNode); 
         
         String sql = " SELECT MEDIAN(temperature) FROM " + table + 
                 " WHERE timed >= " + extract[0] + " AND timed <= " + extract[2] +
                 " AND station >= " + extract[1] + " AND station <= " + extract[3];
         
         if (groupCount >= STATION_COUNT) {
             sql += " GROUP BY station";
         }
         
         if (groupCount > STATION_COUNT) {
             sql += ", CEIL(" + generateGroupByTimed(extract, groupCount) + ")";
         }     
         
         return sql;
    }

    @Override
    protected String generateMonetdb(Node queryNode, int groupCount, int rowCount) throws Exception {
        String[] extract = extractStartEnd(queryNode);        
        String start = extract[0];
        String end = extract[1];
        
        String sql = " SELECT MEDIAN(temperature)";
        
        if (groupCount > STATION_COUNT) {
            String groupBy = generateGroupByTimed(extract, groupCount);
            sql += ", CEIL(" + groupBy + ") AS timedGroup";
        }        
        
        sql += " FROM " + table;
        sql += " WHERE timed >= " + extract[0] + " AND timed <= " + extract[2] +
               " AND station >= " + extract[1] + " AND station <= " + extract[3];
        
        if (groupCount >= STATION_COUNT) {
             sql += " GROUP BY station";
        }
        
        if (groupCount > STATION_COUNT) {
             sql += ", timedGroup";
        } 
        
        return sql;    
    }

    @Override
    protected String generateSqlServer(Node queryNode, int groupCount, int rowCount) throws Exception {
        String[] extract = extractStartEnd(queryNode);        
        
        String groupBy = "";
        if (groupCount == 1) {
            // this is a hack'ish way to force a group by on the whole set as a single group
            groupBy = "FLOOR(station/1000)";
        } else {
            if (groupCount >= STATION_COUNT) {
                groupBy = "station";
            }
            if (groupCount > STATION_COUNT) {
                groupBy += ", FLOOR(" + generateGroupByTimed(extract, groupCount) + ")";
            }
        }
        
        String sql = "SELECT DISTINCT " + groupBy + ", " +
                    " PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY temperature) OVER (PARTITION BY " + groupBy + ") AS median" +
                    " FROM " + table + 
                    " WHERE timed >= " + extract[0] + " AND timed <= " + extract[2] +
                    " AND station >= " + extract[1] + " AND station <= " + extract[3];

        return sql;
    }

    @Override
    protected String generateMySql(Node queryNode, int groupCount, int rowCount) throws Exception {
        String[] extract = extractStartEnd(queryNode);        
        
        String sql = " SELECT MEDIAN(temperature) FROM " + table +
                    " WHERE timed >= " + extract[0] + " AND timed <= " + extract[2] +
                    " AND station >= " + extract[1] + " AND station <= " + extract[3];
        
        if (groupCount >= STATION_COUNT) {
             sql += " GROUP BY station";
         }
        
        if (groupCount > STATION_COUNT) {
             sql += ", CEIL(" + generateGroupByTimed(extract, groupCount) + ")";
         }       
        
        return sql;
    }
    
    protected String generateGroupByTimed (String[] extract, int groupCount) {
        String groupBy = "";
        
        String startStr = extract[0];
        String endStr = extract[2];
        
        long startTimed = Long.parseLong(startStr);
        long endTimed = Long.parseLong(endStr);
        
        // determine range of time between start and end
        long interval = endTimed - startTimed;
        
        // calculate factor to divide timed by
        long divideBy = interval / ((groupCount/STATION_COUNT) - 1);
                
        if (groupCount > STATION_COUNT) {
            groupBy = "timed / " + divideBy;
        }
        
        return groupBy;
    }
    
    protected String[] extractStartEnd(Node queryNode) throws Exception {
        String[] ret = new String[4];
        
        NodeList nodes = queryNode.getChildNodes();
        Node startNode = null;
        Node endNode = null;
        
        
               
        for(int i=0; i < nodes.getLength(); i++) {
            Node currNode = nodes.item(i);
            String nodeName = currNode.getNodeName().toLowerCase();
            
            if (nodeName.equals("start")) {
                startNode = currNode;
            } else if (nodeName.equals("end")) {
                endNode = currNode;
            }
        }
        
        if (startNode == null) {
            throw new Exception("Missing start element in QueryNode; invalid QuerySet file");
        }
        if (endNode == null) {
            throw new Exception("Missing end element in QueryNode; invalid QuerySet file");
        }
        
        NamedNodeMap attr = startNode.getAttributes();
        Node startTimestamp = attr.getNamedItem("timed");
        Node startStation = attr.getNamedItem("station");
        
        if (startTimestamp == null || startStation == null) {
            throw new Exception("Missing start timed or station");
        } 
        
        ret[0] = startTimestamp.getTextContent();
        ret[1] = startStation.getTextContent();
        
        attr = endNode.getAttributes();
        Node endTimestamp = attr.getNamedItem("timed");
        Node endStation = attr.getNamedItem("station");
        
        if (endTimestamp == null || endStation == null) {
            throw new Exception("Missing end timed or station");
        }
        
        ret[2] = endTimestamp.getTextContent();
        ret[3] = endStation.getTextContent();    
        
        return ret;
    }
    
}
