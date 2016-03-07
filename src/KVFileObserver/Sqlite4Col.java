/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import static KVFileObserver.Sqlite4ColumnType.*;
import java.util.Arrays;

/**
 *
 * @author hansone123
 */
enum Sqlite4ColumnType {
    STR , UTF8, UTF16LE, UTF16BE,
    BLOB, NULL, ZERO, ONE, INT,
    REAL, OTHER
    
}
public class Sqlite4Col {
    private String name;
    private byte[] value ;
    private Sqlite4ColumnType type ;
    
    public Sqlite4Col(){ }
    public void setName(String name) {
        this.name = name;
    }
    public void setColumnValue(byte[] valueOfColumn) {
        switch (this.type) {
            case STR:
                byte firstByte = valueOfColumn[0];
                if (firstByte > (byte)0x02) {
                    this.type = UTF8;
                    this.setValue(valueOfColumn);                    
                }
                if (firstByte == (byte)0x00) {
                    this.type = UTF8;
                    this.setValue(Arrays.copyOfRange(valueOfColumn, 1, valueOfColumn.length));
                }
                if (firstByte == (byte)0x01) {
                    this.type = UTF16LE;
                    this.setValue(Arrays.copyOfRange(valueOfColumn, 1, valueOfColumn.length));
                }
                if (firstByte == (byte)0x02) {
                    this.type = UTF16BE;
                    this.setValue(Arrays.copyOfRange(valueOfColumn, 1, valueOfColumn.length));
                }
                break;
            case NULL:
            case ONE:
            case ZERO:
            case OTHER:
                break;
            case BLOB:
            case INT:
            case REAL:
            default:
                this.setValue(valueOfColumn);
                break;
        
        }
    }

    /**
     *
     * @param type
     * enum Sqlite4ColumnType {
     *  KEY,UTF8STR,UTF16LE,UTF16BE,
     *  BLOB, NULL, ZERO, ONE, INT, REAL
     *}
     *
     */
    public void setColumnType(Sqlite4ColumnType type) {
        this.type = type;
    }
    public void setValue(byte[] buf) {
        this.value = buf.clone();
    }
    public void SetColumnTypeAndValue(HeaderOfKValue hdr, byte[] KVpairValueArray) {
        
        this.setColumnType(hdr.type);
        if (hdr.sizeOfValue > 0) {
            this.setColumnValue(Arrays.copyOfRange(KVpairValueArray, hdr.ofstOfValue, hdr.ofstOfValue + hdr.sizeOfValue ));
        }
    }
    public String getName() {
        return this.name;
    }
    public byte[] getValue() {
        return this.value;
    }
    public int getSize() {
        if (this.value == null)
            return 0;
        return this.value.length;
    }

    /**
     *
     * @return type
     * enum Sqlite4ColumnType {
     *  KEY,UTF8STR,UTF16LE,UTF16BE,
     *  BLOB, NULL, ZERO, ONE, INT, REAL
     *}
     */
    public Sqlite4ColumnType getType() {
        return this.type;
    }
    public void show() {
        System.out.println("Column Information");
        System.out.println("  name: " + this.getName());
        System.out.println("  type: " + this.getType());
        System.out.println("  size: " + this.getSize());
    }
    
    public static void main(String args[]) {
        int a[] = {1,9,10,4,5,6,7,8};
        int b[] = Arrays.copyOfRange(a, 5, 8);
                for (int c:b)
                    System.out.println(c + " ");
    }
}
