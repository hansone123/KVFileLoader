/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.dslab.kvloader.Loader;

import com.dslab.kvloader.fileobserver.Observer.FileObserver;
import com.dslab.kvloader.fileobserver.Job.KVFileLoaderJob;

/**
 *
 * @author hansone123
 */
public class KVFileLoader {
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        String dirWatched;
        String schemaDirectory;
        String fileExtesion;
        String dbclientURL;
        if (args.length == 4) {
            dirWatched = args[0];
            fileExtesion = args[1];
            schemaDirectory = args[2];
            dbclientURL = args[3];
        } else {
            System.out.println("Need four parameters: DirectoryWeWatched(first), FileExtension(second), SchemaDirectory(third), dbclientURL(forth)");
            return;
        }
        
        FileObserver fileObserver = new FileObserver();
        if( !fileObserver.setValidDirectoryPath(dirWatched) )  {            
                return;
        }
        fileObserver.setFileExtension(fileExtesion);      
        KVFileLoaderJob job = new KVFileLoaderJob(schemaDirectory, dbclientURL);    
        
        fileObserver.setJob(job);
        fileObserver.keepWatchOnDirectoryAndDoJob(); 
            
    }
}
