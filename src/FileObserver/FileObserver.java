/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package FileObserver;

import FileObserver.Job.Job;
import java.io.File;
import java.util.Collections;
import java.util.Vector;

/**
 *
 * @author hansone123
 */
public class FileObserver {
    
    private Job job;
    private Vector<String> filesName;
    public String directoryPath;
    public String fileExtension;
    
    
    public FileObserver() {
        this.filesName = new Vector();
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
    public void cleanList() {
        this.filesName.clear();
    }
    private boolean dirIsExisted(String dir) {
        
        File directory = new File(dir);
        if ((directory.exists()  && directory.isDirectory())) {
            return true;
        }
        return false;
    }
    
    private String appendDirPath(String file) {
         
        return this.directoryPath + "//" + file;       
        
    }
    private void sortFilesName() {
        Collections.sort(this.filesName);
    }
    private void getMatchedFilesList() {
        
        File directory = new File(this.directoryPath);
        for (String fileName:directory.list()) {           
            if (this.fileExtension.equals("")) {
                this.filesName.addElement(fileName);
            } else {
                if (fileName.contains(this.fileExtension)) {
                    this.filesName.addElement(fileName);
                }
            }
        }
        
        
    }
    public void doJob() {
        
         for (String file:this.filesName) {
            System.out.println("Job start.");
            file = this.appendDirPath(file);
            this.job.execute(file);
            System.out.println("Job done.");
         }
         
    }
    
    public void keepWatchOnDirectoryAndDoJob() {
        System.out.println("Start Watching...");
        while(true) {
            
            this.getMatchedFilesList();
            this.sortFilesName();
            this.doJob();
            this.cleanList();
            
        }
    }
//    public String observeDirectory() {
//        
//    }
    
    
    
            
    
    

    
}
