/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileObserver;

import static KVFileObserver.Sqlite4ColumnType.*;

/**
 *
 * @author hansone123
 */
public class HeaderOfKValue {
    public int ofstOfValue;
    public int sizeOfValue;
    public Sqlite4ColumnType type;
    
    public void setTypeAndSize(int value) {
        
        int valueSize = 0;
        Sqlite4ColumnType valueType;
        
        if (value == 0 ) {
            
            valueType = NULL;
            
        }else if (value == 1 ) {
            
            valueType = ZERO;
            
        }else if ( value == 2 ) {
            
            valueType = ONE;
            
        }
        else if ( value <= 10 ) {
            
            valueType = INT;
            valueSize = value -2;
            
        }else if( value >= 11 && value <= 21) {
            
            valueType = REAL;
            valueSize = value - 9;
            
        }else if ( value >= 22 ) {
            
            int subtype = 0;
            subtype = (value-22)%4;
            valueSize = (value-22)/4;
            switch (subtype) {
                case 0:
                    valueType = STR;
                    break;
                case 1:
                    valueType = BLOB;
                    break;
                default:
                    valueType = OTHER;
                    valueSize = 0;
                    break;
            }
        }else {
            valueType = OTHER;
        }
        this.sizeOfValue = valueSize;
        this.type = valueType;
    }
    public void setOfstOfValue(int value) {
        this.ofstOfValue = value;
    }
    public void show() {
        System.out.println("---------------------------");
        System.out.println("type: " + this.type);
        System.out.println("ofst of data: " + this.ofstOfValue);
        System.out.println("size of data: " + this.sizeOfValue);
        
    }
}
