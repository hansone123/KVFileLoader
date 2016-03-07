/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class Sqlite4Record {
    private int tableID;
    private String tableName;
    private Vector<Sqlite4Col> columns;
    public Sqlite4Record(){
        this.tableID = -1;
        this.tableName = "";
        this.columns = new Vector<Sqlite4Col>();
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public void setTableID(int tableID) {
        this.tableID = tableID;
    }
    public void addColumn(Sqlite4Col col) {
        columns.addElement(col);
    }
    public String getTableName() {
        return this.tableName;
    }
    public int getTableID() {
        return this.tableID;
    }
    public Vector<Sqlite4Col> getColumns() {
        return this.columns;
    }
    
}
