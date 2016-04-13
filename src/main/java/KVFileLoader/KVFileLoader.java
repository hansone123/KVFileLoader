/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileLoader;

import FileObserver.FileObserver;
import FileObserver.Job.KVFileLoaderJob;

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
        String dbclient;
        if (args.length == 4) {
            dirWatched = args[0];
            fileExtesion = args[1];
            schemaDirectory = args[2];
            dbclient = args[3];
        } else {
            System.out.println("Need four parameters: DirectoryWeWatched(first), FileExtension(second), SchemaDirectory(third), dbclientURL(forth)");
            return;
        }
        
        FileObserver fileObserver = new FileObserver();
        if( !fileObserver.setValidDirectoryPath(dirWatched) )  {            
                return;
        }
        fileObserver.setFileExtension(fileExtesion);      
        KVFileLoaderJob job = new KVFileLoaderJob();        
        job.setUp(schemaDirectory, dbclient);
        
        fileObserver.setJob(job);
        fileObserver.keepWatchOnDirectoryAndDoJob(); 
            
    }
}
