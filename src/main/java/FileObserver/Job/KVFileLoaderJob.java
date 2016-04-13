/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver.Job;

import Dataset.KVDataset;
import FileObserver.FileObserver;
import FileObserver.Job.Job;
import KVFileObserver.KVFile;
import KVFileObserver.Sqlite4Col;
import KVFileObserver.Sqlite4Decoder;
import KVFileObserver.Sqlite4Record;
import KVFileObserver.TableSchema;
import com.cdclab.loader.core.LoaderConf;
import com.cdclab.loader.dataset.Dataset.Column;
import com.cdclab.loader.dbClient.DBClient;
import com.cdclab.loader.dbClient.PhoenixDBClient;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.JDBCType;
import java.sql.SQLException;
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
    private PhoenixDBClient phoenixDbClient;
    private ArrayList<TableSchema> schemas;
    public KVFileLoaderJob() {
        this.schemas = new ArrayList();
    }
    public boolean setUp(String schemaDir, String dbURL) {
        this.loadTableSchema(schemaDir);
        try {
            this.initialDBClient(dbURL);
        } catch (ClassNotFoundException|SQLException ex) {
           return false; 
        }
        return true;
    }
    private KVFile readAndRenderKVFile(String fileName) throws FileNotFoundException, IOException {
           
        BufferedInputStream file = new BufferedInputStream(new FileInputStream(fileName));
        byte[] buf = new byte[file.available()];
        file.read(buf, 0, file.available());
        KVFile kvfile = new KVFile(fileName, buf);   
        
        System.out.println("Open file: " + kvfile.getName());
        System.out.println("file size: " + kvfile.getSize());
        
        return kvfile;
    }
    private void initialDBClient(String url) throws ClassNotFoundException, SQLException {
        Configuration config = HBaseConfiguration.create();
        //config.set("phoenix.query.dateFormatTimeZone", "GMT+6");
        config.set(LoaderConf.LOADER_MODE, "SQL_QUERY");
        LoaderConf conf= new LoaderConf(config);
        this.phoenixDbClient= new PhoenixDBClient(url, conf.getWriteThreadNumber());
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
        
        records.stream().forEach((record) -> {
            datasets.stream().filter((dataset) -> (dataset.getTableID() == record.getTableID())).forEach((dataset) -> {
                this.renderDataset(dataset, record);
            });
        records.remove(record);
        });
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
            datasets.add(new KVDataset(schema.getColsName(), schema.getColsType(), schema.getTableID()));
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
    private void deleteFile(String fileName) {
        File file = new File(fileName);
        file.delete();
    }
    @Override
    public void execute(String fileName) {
        KVFile kvfile = new KVFile(fileName);        
        kvfile.readAndRenderKVFile();
        List<KVDataset> datasets = this.createAndRenderDataset(kvfile);
        
        for (KVDataset dataset:datasets) {
            
        }
        
        this.deleteFile(fileName);
    }
    
}
