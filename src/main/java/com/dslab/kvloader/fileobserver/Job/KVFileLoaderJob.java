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
import com.dslab.kvloader.KVdata.SchemaCache;
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
import java.util.Iterator;
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
//    private final ArrayList<TableSchema> schemas;
    private SchemaCache schemaCache;
    
    public KVFileLoaderJob() {
    }
    public boolean setUp(String schemaDir, String dbURL) {
        this.loadTableSchema(schemaDir);
        try {
            this.initPhoenixClient(dbURL);
        } catch (ClassNotFoundException|SQLException ex) {
           System.out.println("Phoenix client create failed.");
           return false; 
        }
        return true;
    }
    private void initPhoenixClient(String url) throws ClassNotFoundException, SQLException {
        Configuration config = HBaseConfiguration.create();
        //config.set("phoenix.query.dateFormatTimeZone", "GMT+6");
        config.set(LoaderConf.LOADER_MODE, "SQL_QUERY");
        loaderConf= new LoaderConf(config);
        this.phoenixDbClient= new PhoenixDBClient(url, loaderConf.getWriteThreadNumber());
    }
    private void loadTableSchema(String dirPath) {
        List<TableSchema> schemas = new ArrayList();
        File directory = new File(dirPath);
        for (String file:directory.list()) {
            System.out.println("Find schema file: "+dirPath + file);
            TableSchema schema = new TableSchema();
            if (schema.readFromFile(dirPath + "//" + file)) {
                schema.show();
                schemas.add(schema);
            }
        }
        this.schemaCache = new SchemaCache(schemas);
    }
    private  List<KVDataset> createAndRenderDataset(KVFile kvfile) {
        
        List<Sqlite4Record> records = kvfile.toSqlite4Records();
        List<KVDataset> datasets = this.createDatasets(this.schemaCache.iterator());
        this.renderDatasets(datasets, records);
        return datasets;
        
    }
    private void renderDatasets(List<KVDataset> datasets, List<Sqlite4Record> records) {
        
        for (Sqlite4Record record:records) {
            for (KVDataset dataset:datasets) {
                int dataset_id = dataset.getTableID();
                if (dataset_id == record.getTableID()) {
                    TableSchema schema = this.schemaCache.get(dataset_id);
                    dataset.append(record, schema);
                }
            }
        }
    }

    private List<KVDataset> createDatasets(Iterator<TableSchema> schemas) {
        List<KVDataset> datasets = new ArrayList();
        while(schemas.hasNext()) {
            TableSchema schema = schemas.next();
            datasets.add(new KVDataset(schema.getColsName(), schema.getColsType(), schema.getTableID(), schema.getTableName()));
        }
        return datasets;
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
