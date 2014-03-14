/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package nl.pallett.dbtestingtool;

import nl.pallett.dbtestingtool.gui.SelectQueriesScreen;

/**
 *
 * @author Dennis
 */
public class DbTestingTool {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        TestRunner testRunner = new TestRunner();
        
        SelectQueriesScreen startScreen = new SelectQueriesScreen(testRunner);
        startScreen.setVisible(true);
    }
}
