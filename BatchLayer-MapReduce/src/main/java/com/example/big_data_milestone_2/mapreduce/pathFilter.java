package com.example.big_data_milestone_2.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;

public class pathFilter extends Configured implements PathFilter {
    private String start;
    private String end;
    private FileSystem fs;

    public boolean accept(Path path) {

        System.out.println(path.toString());
        try {
            if (fs.isDirectory(path)) {
                return true;
            }
        } catch (IOException e) {}

        long file = Long.parseLong(path.getName().split(".csv")[0]);
        long startLong = Long.parseLong(start);
        long endLong = Long.parseLong(end);
        return (file <= startLong || file <= endLong);
    }

    public void setConf(Configuration conf) {
        if (null != conf) {
            this.start = conf.get("start");
            this.end = conf.get("end");
            try {
                this.fs = FileSystem.get(conf);
            } catch (IOException e) {}
        }
    }
}
