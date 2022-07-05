package org.example;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

public class SpeedViewEdit {

    void deleteOldView(String filePath) throws IOException {
        File directoryToBeDeleted = new File(filePath);
        FileUtils.deleteDirectory(directoryToBeDeleted);

        //the below deletes only files inside a given directory

        /*File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                if(file.isDirectory()){
                    deleteOldView(file.getPath());
                }else {
                    file.delete();
                }
            }
        }*/
    }

}
