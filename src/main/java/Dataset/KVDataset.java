/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Dataset;

import com.cdclab.loader.dataset.SimpleDataset;
import java.sql.JDBCType;
import java.util.List;
/**
 *
 * @author hansone123
 */
public class KVDataset extends SimpleDataset {
    private int tableID;
    public KVDataset(List<String> columns, List<JDBCType> types, int table_id) {
        super(columns, types);
        this.tableID = table_id;
    }
    public int getTableID() {
        return this.tableID;
    }
}
