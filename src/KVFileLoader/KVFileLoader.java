/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package KVFileLoader;

import FileObserver.FileObserver;
import FileObserver.Job.KVFileLoaderJob;
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
        String dirWatched;
        String schemaDirectory;
        String fileExtesion;
        if (args.length == 3) {
            dirWatched = args[0];
            fileExtesion = args[1];
            schemaDirectory = args[2];
        } else {
            System.out.println("Need third parameters: DirectoryWeWatched(first), FileExtension(second), SchemaDirectory(third)");
            return;
        }
        
        FileObserver fileObserver = new FileObserver();
        if( !fileObserver.setValidDirectoryPath(dirWatched) )  {            
                return;
        }
        fileObserver.setFileExtension(fileExtesion);      
        KVFileLoaderJob job = new KVFileLoaderJob();
        job.loadTableSchema(schemaDirectory);
        fileObserver.setJob(job);
        fileObserver.keepWatchOnDirectoryAndDoJob();    
            
    }
}
