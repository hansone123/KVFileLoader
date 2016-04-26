/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvLoader.Dataset;

import com.cdclab.loader.dataset.Dataset;
import com.cdclab.loader.dataset.SimpleDataset;
import com.dslab.kvloader.KVdata.Sqlite4Col;
import com.dslab.kvloader.KVdata.Sqlite4Record;
import com.dslab.kvloader.KVdata.TableSchema;
import java.sql.JDBCType;
import java.util.ArrayList;
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
    public void append(Sqlite4Record record, TableSchema schema) {
        List<Column> columns = new ArrayList();
       
        List<JDBCType> columnType = schema.getColsType();
        List<String> columnName = schema.getColsName();
        
        record.getColumns().stream().map((col) -> {
            int index = col.getIndex();
            JDBCType type = columnType.get(index);
            col.setName(columnName.get(index));
            Column temp = createColumnForDataset(type, col);
            return temp;
        }).filter((temp) -> (temp != null)).forEach((temp) -> {
            columns.add(temp);
        });
        this.append(columns);
    }
    private Column createColumnForDataset(JDBCType type, Sqlite4Col sqlite4Column) {
        Column datasetColumn = null;
        try{
            switch(type) {
                case INTEGER:
                    datasetColumn = new Column(JDBCType.INTEGER, sqlite4Column.getName(), Integer.valueOf(sqlite4Column.toString()), sqlite4Column.getIndex());
                    break;
                case FLOAT:
                    datasetColumn = new Column(JDBCType.FLOAT, sqlite4Column.getName(), Float.valueOf(sqlite4Column.toString()), sqlite4Column.getIndex());
                    break;
                case DOUBLE:
                    datasetColumn = new Column(JDBCType.DOUBLE, sqlite4Column.getName(), Double.valueOf(sqlite4Column.toString()), sqlite4Column.getIndex());
                    break;
                case VARCHAR:
                    datasetColumn = new Column(JDBCType.VARCHAR, sqlite4Column.getName(), sqlite4Column.toString(), sqlite4Column.getIndex());
                    break;
                case BLOB:
                    datasetColumn = new Column(JDBCType.BINARY, sqlite4Column.getName(), sqlite4Column.getValue(), sqlite4Column.getIndex());
                    break;
                default:
                    break;
             }
        } catch(Exception e) {
            return null;
        }
        return datasetColumn;
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
