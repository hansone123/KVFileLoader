/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.KVdata;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hansone123
 */
public class Sqlite4Record {
    private int tableID;
    private String tableName;
    private ArrayList<Sqlite4Col> columns;
    public Sqlite4Record(){
        this.tableID = -1;
        this.tableName = "";
        this.columns = new ArrayList();
    }
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public void setTableID(int tableID) {
        this.tableID = tableID;
    }
    public void addColumn(Sqlite4Col col) {
        columns.add(col);
    }
    public String getTableName() {
        return this.tableName;
    }
    public int getTableID() {
        return this.tableID;
    }
    public int getColNum() {
        return this.columns.size();
    }
    public List<Sqlite4Col> getColumns() {
        return this.columns;
    }
    public void show() {
        System.out.println("table id: " + this.getTableID());
        System.out.println("table name: " + this.getTableName());
        for (Sqlite4Col col:this.getColumns()) {
            col.show();
        }
    }
}
