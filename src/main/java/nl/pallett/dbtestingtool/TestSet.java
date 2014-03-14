/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import java.io.IOException;
import java.util.ArrayList;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 *
 * @author Dennis
 */
public class TestSet {
    private DocumentBuilderFactory dbf; 
    
    protected String filename;
    
    protected String source;
    
    protected String name;
    
    protected int size = 0;
        
    protected ArrayList<QuerySet> querySetList;    
    
    public TestSet () {
        // Make an  instance of the DocumentBuilderFactory
        dbf = DocumentBuilderFactory.newInstance();
        
        querySetList = new ArrayList<QuerySet>();
    }
    
    public int getSetCount () {
        return this.querySetList.size();
    }
    
    public ArrayList<QuerySet> getQuerySetList () {
        return this.querySetList;
    }
    
    public String getSource () {
        return this.source;
    }
    
    public String getName () {
        return this.name;
    }
    
    public int getSize () {
        return size;
    }
    
    public String getFilename () {
        return this.filename;
    }
    
    public boolean loadFile(String filename) throws Exception {
        this.filename = filename;
        
        Document dom = null;

        try {
            DocumentBuilder db = dbf.newDocumentBuilder();
            dom = db.parse(new InputSource(filename));
        } catch (ParserConfigurationException ex) {
            // ignore
        } catch (SAXException ex) {
            // ignore
        } catch (IOException ex) {
            // ignore
        }
        
        // nothing parsed? throw exception
        if (dom == null) {
            throw new Exception("Unable to parse XML of QuerySet file");
            
        }
        
        Element doc = dom.getDocumentElement();
        
        NodeList list = doc.getElementsByTagName("dataset");
        
        if (list.getLength() < 1) {
            throw new Exception("QuerySet file is missing dataset info element");
        }
        
        Node node = list.item(0);
        NamedNodeMap attr = node.getAttributes();
        
        Node sourceNode = attr.getNamedItem("source");
        Node nameNode = attr.getNamedItem("name");
        Node sizeNode = attr.getNamedItem("size");
        
        if (sourceNode == null || nameNode == null || sizeNode == null) {
            throw new Exception("Dataset info element in QuerySet is missing one or more required attributes: source, name or size");
        }
        
        source = sourceNode.getTextContent();
        name = nameNode.getTextContent();
        
        try {
            size = Integer.parseInt(sizeNode.getTextContent());
        } catch (NumberFormatException e) {
            throw new Exception("Dataset info element in QuerySet has an invalid size attribute; must be an integer!");
        }
        
        NodeList setList = doc.getElementsByTagName("set");
        
        for(int i=0; i < setList.getLength(); i++) {
            Node setNode = setList.item(i);
            NamedNodeMap setAttr = setNode.getAttributes();
            
            Node setNameNode = setAttr.getNamedItem("name");
            Node setSizeNode = setAttr.getNamedItem("size");
            
            if (setNameNode == null || setSizeNode == null) {
                throw new Exception("Set element is missing one or more required attributes: name or size");
            }
            
            String setName = setNameNode.getTextContent();
            int setSize = -1;
            try {
                setSize = Integer.parseInt(setSizeNode.getTextContent());
            } catch (NumberFormatException e) {
                throw new Exception("Set element has an invalid size attribute; must be an integer!");
            }
            
            NodeList children = setNode.getChildNodes();
            ArrayList<Node> queryNodes = new ArrayList<Node>();
            for(int j=0; j < children.getLength(); j++) {
                Node queryNode = children.item(j);
                if (queryNode.getNodeName().toLowerCase().equals("query")) {
                    queryNodes.add(queryNode);
                }
            }  
            
            QuerySet querySet = new QuerySet(setName, setSize, queryNodes);
            querySetList.add(querySet);
        }
                        
        return true;
    }
    
    public String toString () {
        return this.source + "/" + this.name;
    }
    
}
