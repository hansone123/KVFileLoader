/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.fileobserver.Job;

import com.dslab.kvLoader.Dataset.KVDataset;
import com.dslab.kvloader.fileobserver.Observer.FileObserver;
import com.dslab.kvloader.fileobserver.Job.Job;
import com.dslab.kvloader.KVdata.KVFile;
import com.dslab.kvloader.KVdata.Sqlite4Col;
import com.dslab.kvloader.KVdata.Sqlite4Decoder;
import com.dslab.kvloader.KVdata.Sqlite4Record;
import com.dslab.kvloader.KVdata.TableSchema;
import com.cdclab.loader.core.LoaderConf;
import com.cdclab.loader.dataset.Dataset.Column;
import com.cdclab.loader.dbClient.DBClient;
import com.cdclab.loader.dbClient.PhoenixDBClient;
import com.cdclab.loader.dbClient.TableName;
import com.cdclab.loader.dbClient.Writer;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

/**
 *
 * @author hansone123
 */
public class KVFileLoaderJob implements Job{
    private LoaderConf loaderConf;
    private DBClient phoenixDbClient;
    private final ArrayList<TableSchema> schemas;
    
    public KVFileLoaderJob(String schemaDir, String dbURL) {
        this.schemas = new ArrayList();
        this.loadTableSchema(schemaDir);
        try {
            this.initialDBClient(dbURL);
        } catch (ClassNotFoundException|SQLException ex) {
           System.out.println("Phoenix client create failed.");
        }
    }
//    public boolean setUp(String schemaDir, String dbURL) {
//        this.loadTableSchema(schemaDir);
//        try {
//            this.initialDBClient(dbURL);
//        } catch (ClassNotFoundException|SQLException ex) {
//           System.out.println("Phoenix client create failed.");
//           return false; 
//        }
//        return true;
//    }
    private void initialDBClient(String url) throws ClassNotFoundException, SQLException {
        Configuration config = HBaseConfiguration.create();
        //config.set("phoenix.query.dateFormatTimeZone", "GMT+6");
        config.set(LoaderConf.LOADER_MODE, "SQL_QUERY");
        loaderConf= new LoaderConf(config);
        this.phoenixDbClient= new PhoenixDBClient(url, loaderConf.getWriteThreadNumber());
    }
    private void loadTableSchema(String dirPath) {
        
        File directory = new File(dirPath);
        for (String file:directory.list()) {
            TableSchema schema = new TableSchema();
            if (schema.readFromFile(dirPath + "//" + file)) {
                schema.show();
                this.schemas.add(schema);
            }
        }
        
    }
    private  List<KVDataset> createAndRenderDataset(KVFile kvfile) {
        
        List<Sqlite4Record> records = kvfile.toSqlite4Records();
        List<KVDataset> datasets = this.createDatasets(this.getTablesSchema());
        
        this.renderDatasets(datasets, records);
        return datasets;
        
    }
    private void renderDatasets(List<KVDataset> datasets, List<Sqlite4Record> records) {
        
        for (Sqlite4Record record:records) {
            for (KVDataset dataset:datasets) {
                if (dataset.getTableID() == record.getTableID()) {
                    this.renderDataset(dataset, record);
                }
            }
        }
    }
    private void renderDataset(KVDataset dataset, Sqlite4Record record) {
        
        List<Column> columns = new ArrayList();
        TableSchema schema = this.schemas.get(dataset.getTableID());
        List<JDBCType> columnType = schema.getColsType();
        List<String> columnName = schema.getColsName();
        record.getColumns().stream().map((col) -> {
            int index = col.getIndex();
            JDBCType type = columnType.get(index);
            col.setName(columnName.get(index));
            Column temp = buildColumnForDatasetType(type, col);
            return temp;
        }).filter((temp) -> (temp != null)).forEach((temp) -> {
            columns.add(temp);
        });
        dataset.append(columns);
    }
    private Column buildColumnForDatasetType(JDBCType type, Sqlite4Col input) {
        Column column = null;
        try{
            switch(type) {
                case INTEGER:
                    column = new Column(JDBCType.INTEGER, input.getName(), Integer.valueOf(input.toString()), input.getIndex());
                    break;
                case FLOAT:
                    column = new Column(JDBCType.FLOAT, input.getName(), Float.valueOf(input.toString()), input.getIndex());
                    break;
                case DOUBLE:
                    column = new Column(JDBCType.DOUBLE, input.getName(), Double.valueOf(input.toString()), input.getIndex());
                    break;
                case VARCHAR:
                    column = new Column(JDBCType.VARCHAR, input.getName(), input.toString(), input.getIndex());
                    break;
                case BLOB:
                    column = new Column(JDBCType.BINARY, input.getName(), input.getValue(), input.getIndex());
                    break;
                default:
                    break;
             }
        } catch(Exception e) {
            return null;
        }
        return column;
    }
    private List<KVDataset> createDatasets(List<TableSchema> schemas) {
        List<KVDataset> datasets = new ArrayList();
        schemas.stream().forEach((schema) -> {
            datasets.add(new KVDataset(schema.getColsName(), schema.getColsType(), schema.getTableID(), schema.getTableName()));
        });
        return datasets;
    }
    private List<TableSchema> getTablesSchema() {
        return this.schemas;
    }
    private TableSchema getTableSchemaWithId(int id) {
        for (TableSchema schema:this.schemas) {
            if (schema.getTableID() == id) {
                return schema;
            }
        }
        return null;
    }
    private boolean sendAQuery(Sqlite4Record record) {
        
        for (Sqlite4Col col : record.getColumns()) {
            col.show();
            Sqlite4Decoder decoder = new Sqlite4Decoder();
            System.out.println("  value: " + record.toString());
        }
        return true;
    }
    private void deleteFile(String filePath) {
        File file = new File(filePath);
        System.out.println("Delete " + filePath + "...");
        if (!file.delete()) {
            System.out.println("failed.");
        }        
        System.out.println("Done.");
        
    }
    @Override
    public void execute(final String fileName) {
        KVFile kvfile = new KVFile();        
        kvfile.readAndRenderKVFile(fileName);
        List<KVDataset> datasets = this.createAndRenderDataset(kvfile);
        
        int successWrite = 0;
        for (KVDataset dataset:datasets) {
            dataset.showInfo();
//            successWrite += this.writeDataset(dataset);
        }
        System.out.println("Num of datasets " + datasets.size());
        System.out.println("Success Write: " + successWrite);
        
        this.deleteFile(fileName);
    }
    public int writeDataset(KVDataset dataset) {
        final String tableName = dataset.getTableName();
        try(Writer writer= new Writer(phoenixDbClient, loaderConf, new TableName(tableName), loaderConf.getBatchNumber())){
//            writer.setPreWriteOperation((Column col)->{
//                if(col.getKey()=="Name"){
//                    System.out.println("preWrite Name: "+col.getValue().orElse("NULL"));
//                }
//            });
//            
//            writer.setPostWriteOperation((Column col)->{
//                if(col.getKey()=="Name"){
//                    System.out.println("postWrite Name: "+col.getValue().orElse("NULL"));
//                }
//            });
//            
            
            writer.write(dataset);
        } catch (SQLException ex) {
            System.out.println("SQLException: dataset of " + tableName + "tableName write failed.");
            return 0;
        } catch (IOException ex) {
            System.out.println("IOException: dataset of " + tableName + "tableName write failed.");
            return 0;
        }
        return 1;
    }
}
