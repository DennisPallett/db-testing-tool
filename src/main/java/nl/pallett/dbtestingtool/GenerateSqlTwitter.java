/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.util.ArrayList;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author Dennis
 */
public class GenerateSqlTwitter extends GenerateSql {
    public final int SRID = 4326;
    
    @Override
    protected String generatePostgres(Node queryNode, int groupCount, int rowCount) throws Exception {
        QueryParams params = new QueryParams(queryNode);
        
        
        
        String sql = "";
        
        if (groupCount == 1) {
            sql = " SELECT MEDIAN(len) FROM " + this.table + " WHERE " +
                  " timed >= " + params.getStartTime() + " AND timed <= " + params.getEndTime() +
                  " AND ST_Intersects(coordinates, ST_GeomFromText('" + params.getBoundingBoxAsWkt() + "', " + SRID + "))";
        } else {
            QueryGroups groups = new QueryGroups(groupCount, params);
            ArrayList<QueryParams> groupList = groups.getList();
            
            for(int i=0; i < groupList.size(); i++) {
                QueryParams group = groupList.get(i);
                
                if (i > 0) {
                    sql += " UNION ";
                }
                
                sql += " SELECT " + (i+1) + " AS groupnr, MEDIAN(LEN) FROM " + this.table + " WHERE " +
                       " timed >= " + params.getStartTime() + " AND timed <= " + params.getEndTime() +
                       " AND ST_Intersects(coordinates, ST_GeomFromText('" + group.getBoundingBoxAsWkt() + "', " + SRID + "))";
            }
        }
        
        return sql;
    }

    @Override
    protected String generateMonetdb(Node queryNode, int groupCount, int rowCount) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String generateSqlServer(Node queryNode, int groupCount, int rowCount) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    protected String generateMySql(Node queryNode, int groupCount, int rowCount) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
    
    private class QueryGroups {
        protected int groupCount;
        
        protected QueryParams params;
        
        protected ArrayList<QueryParams> groupList;
        
        public QueryGroups (int groupCount, QueryParams params) {
            this.groupCount = groupCount;
            this.params = params;
            
            if (groupCount == 10) {
                // group x in 5 and y into 2 to generate a 5x2 grid
                this.calculateGroups(5, 2);
            } else if (groupCount == 100) {
                // group x in 10 and y into 10 to generate a 10x10 grid
                this.calculateGroups(10, 10);
            } else {
                throw new UnsupportedOperationException("Groupcount of " + groupCount + " is not yet supported!");
            }
        }
        
        public ArrayList<QueryParams> getList () {
            return this.groupList;
        }
        
        protected void calculateGroups (int groupsX, int groupsY) {
            double IntervalX = params.getIntervalX();
            double IntervalY = params.getIntervalY();
            
            double stepX = IntervalX / groupsX;
            double stepY = IntervalY / groupsY;
            
            groupList = new ArrayList<QueryParams>();
            
            double minX = params.getMinX();
            for(int i=0; i < groupsX; i++) {
                double maxX = minX + stepX;
                
                double minY = params.getMinY();
                for (int j=0; j < groupsY; j++) {
                    double maxY = minY + stepY;
                    
                    QueryParams group = new QueryParams(
                                            params.getStartTime(), 
                                            params.getEndTime(),
                                            minX,
                                            maxX,
                                            minY,
                                            maxY
                                        );
                    
                    groupList.add(group);
                    
                    minY = maxY;
                }
                
                minX = maxX;
            }
            
        }
        
    }
    
    private class QueryParams {        
        protected int startTime;
        
        protected int endTime;
        
        protected double minX;
        protected double maxX;
        
        protected double minY;
        protected double maxY;
        
        public QueryParams(Node queryNode) throws Exception {
            this.parseQueryNode(queryNode);
        }
        
        public QueryParams (int startTime, int endTime, double minX, double maxX, double minY, double maxY) {
            this.startTime = startTime;
            this.endTime = endTime;
            
            this.minX = minX;
            this.maxX = maxX;
            
            this.minY = minY;
            this.maxY = maxY;
        }
        
        protected void parseQueryNode(Node queryNode) throws Exception {
            NodeList nodes = queryNode.getChildNodes();
            Node timedNode = null;
            Node bboxNode = null;    

            for(int i=0; i < nodes.getLength(); i++) {
                Node currNode = nodes.item(i);
                String nodeName = currNode.getNodeName().toLowerCase();

                if (nodeName.equals("timed")) {
                    timedNode = currNode;
                } else if (nodeName.equals("bbox")) {
                    bboxNode = currNode;
                }
            }
            
            if (timedNode == null) {
                throw new Exception("Missing timed element in QueryNode");
            }
            if (bboxNode == null) {
                throw new Exception("Missing bbox element in QueryNode");
            }
            
            NamedNodeMap attr = timedNode.getAttributes();
            Node startTimeAttr = attr.getNamedItem("start");
            Node endTimeAttr = attr.getNamedItem("end");
            
            if (startTimeAttr == null) throw new Exception("Missing start attribute on timed element in QueryNode");
            if (endTimeAttr == null) throw new Exception("Missing end attribute on timed element in QueryNode");
            
            this.startTime = Integer.parseInt(startTimeAttr.getTextContent());
            this.endTime = Integer.parseInt(endTimeAttr.getTextContent());
            
            attr = bboxNode.getAttributes();
            Node minYAttr = attr.getNamedItem("miny");
            Node minXAttr = attr.getNamedItem("minx");
            Node maxYAttr = attr.getNamedItem("maxy");
            Node maxXAttr = attr.getNamedItem("maxx");
            
            if (minYAttr == null) throw new Exception("Missing miny attribute on bbox element in QueryNode");
            if (minXAttr == null) throw new Exception("Missing minx attribute on bbox element in QueryNode");
            if (maxYAttr == null) throw new Exception("Missing maxy attribute on bbox element in QueryNode");
            if (maxXAttr == null) throw new Exception("Missing maxx attribute on bbox element in QueryNode");
            
            this.minY = Double.parseDouble(minYAttr.getTextContent());
            this.minX = Double.parseDouble(minXAttr.getTextContent());
            this.maxY = Double.parseDouble(maxYAttr.getTextContent());
            this.maxX = Double.parseDouble(maxXAttr.getTextContent());
        }

        /**
         * @return the startTime
         */
        public int getStartTime() {
            return startTime;
        }

        /**
         * @return the endTime
         */
        public int getEndTime() {
            return endTime;
        }

        /**
         * @return the minX
         */
        public double getMinX() {
            return minX;
        }
        
        /**
         * @return the maxX
         */
        public double getMaxX() {
            return maxX;
        }

        /**
         * @return the minY
         */
        public double getMinY() {
            return minY;
        }
        
        public String getBoundingBoxAsWkt () {
            return "POLYGON((" + 
                    minX + " " + minY + "," +
                    maxX + " " + minY + "," +
                    maxX + " " + maxY + "," +
                    minX + " " + maxY + "," +
                    minX + " " + minY + "" +
                    "))";                    
        }

        /**
         * @return the maxY
         */
        public double getMaxY() {
            return maxY;
        }
        
        public double getIntervalX() {
            return maxX - minX;
        }
        
        public double getIntervalY() {
            return maxY - minY;
        }
        
    }
    
}
