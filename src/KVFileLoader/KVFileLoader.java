/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileLoader;

import KVFileObserver.KVFileObserver;
import java.io.IOException;

/**
 *
 * @author hansone123
 */
public class KVFileLoader {
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        
        KVFileObserver kvfileObserver = new KVFileObserver();
        if( !kvfileObserver.setValidDirectoryPath("/tmp/KVoutput") )  {            
            return;
        }
        
            kvfileObserver.setFileExtension(".kv");   
        
        kvfileObserver.setFileExtension("");
        kvfileObserver.keepWatchOnDirectoryAndDoJob();
            
    }
}
