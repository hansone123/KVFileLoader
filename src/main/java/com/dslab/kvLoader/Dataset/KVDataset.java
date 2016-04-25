/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvLoader.Dataset;

import com.cdclab.loader.dataset.Dataset;
import com.cdclab.loader.dataset.SimpleDataset;
import java.sql.JDBCType;
import java.util.Iterator;
import java.util.List;
/**
 *
 * @author hansone123
 */
public class KVDataset extends SimpleDataset {
    final private int tableID;
    final private String tableName;
    public KVDataset(List<String> columns, List<JDBCType> types, int table_id, String table_Name) {
        super(columns, types);
        this.tableID = table_id;
        this.tableName = table_Name;
    }
    public int getTableID() {
        return this.tableID;
    }
    public String getTableName() {
        return this.tableName;
    }
    public void showInfo() {
        System.out.println("\nDataset:");
        System.out.println("table_id: " + this.getTableID());
        System.out.println("table_name: " + this.getTableName());
        System.out.println(this.toString());
        Iterator<Dataset.Row> rowItr= this.iterator();        
        while(rowItr.hasNext()){
            Dataset.Row row= rowItr.next();
            Iterator<Column> colItr= row.iterator();
            while(colItr.hasNext()){
                Column col = colItr.next();
               System.out.println(col.toString());
            }
        }
        System.out.println();
    }
}
