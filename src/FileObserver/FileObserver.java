/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver;

import FileObserver.Job.Job;
import java.io.File;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class FileObserver {
    
    private Job job;
    private String fileName;
    public String directoryPath;
    public String fileExtension;
    
    public FileObserver() {
        this.directoryPath = "";
        this.fileExtension = "";
    }
    public void setJob(Job job) {
        this.job = job;
    }
    public void setFileExtension(String extension) {
        this.fileExtension = extension;
        System.out.println("FileObserver: Extension is set to \"" + this.fileExtension + "\"");
    }
    public boolean setValidDirectoryPath(String dirPath) {
        if (!this.dirIsExisted(dirPath)) {
            System.out.println("FileObserver: directory is not existed.");
            return false;
        }  
        this.directoryPath = dirPath;    
        System.out.println("FileObserver: directory is set to \"" + this.directoryPath + "\"");
        return true;
    }
    
    private boolean dirIsExisted(String dir) {
        
        File directory = new File(dir);
        if ((directory.exists()  && directory.isDirectory())) {
            return true;
        }
        return false;
    }
    
    private void chooseFile() {
        if (this.dirIsEmpty()) {
//            System.out.println("Directory is empty");
            this.fileName = "";
            return;
        }
        
        Vector<String> files = this.getMatchedFilesName();
        
        if (files.isEmpty()) {
            this.fileName = "";
            return;   
        }
        
        String result;
        result = files.firstElement();
        for (String file : files) {
            if (result.compareTo(file) > 0)
                result = file;
        }
        result = this.directoryPath + "//" + result;       
        
        this.fileName = result;
        System.out.println("Choose file: " + result);
    }
    private Vector<String> getMatchedFilesName() {
        
        File directory = new File(this.directoryPath);
        Vector<String> matchedFiles  = new Vector<String>();
        
        for (String fileName:directory.list()) {           
            if (this.fileExtension.equals("")) {
                matchedFiles.addElement(fileName);
            } else {
                if (fileName.contains(this.fileExtension)) {
                    matchedFiles.addElement(fileName);
                }
            }
        }
        return matchedFiles;
    }
    private boolean dirIsEmpty() {
        File directory = new File(this.directoryPath);
        String[] allFiles = directory.list();
        return allFiles.length < 1;
    }
    public void doJob() {
         if (this.fileName.equals("")) {
             return;
         }
         System.out.println("Job start.");
         this.job.execute(this.fileName);
         
        System.out.println("Job done.");
    }

    public void keepWatchOnDirectoryAndDoJob() {
        System.out.println("Start Watching...");
        while(true) {
            this.chooseFile();
            this.doJob();
        }
    }
//    public String observeDirectory() {
//        
//    }
    
    
    
            
    
    

    
}
