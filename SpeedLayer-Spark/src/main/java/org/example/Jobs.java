package org.example;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;


public class Jobs implements Runnable{
    StreamingQuery sq;
    String path;
    int ID;

    public void setPath(String path) {
        this.path = path;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    void createJob() throws StreamingQueryException, IOException {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("SpeedLayer").master("local").getOrCreate();
        StructType schema = new StructType()
                .add("serviceName", DataTypes.StringType)
                .add("stamp", DataTypes.LongType)
                .add("cpu",DataTypes.DoubleType)
                .add("ram_Total",DataTypes.DoubleType)
                .add("ram_Free",DataTypes.DoubleType)
                .add("disk_Total",DataTypes.DoubleType)
                .add("disk_Free",DataTypes.DoubleType);

        Dataset<Row> rawData = spark.readStream()
                .format("json")
                .schema(schema)
                .json("/home/hadoopuser/speedlayer/data");
        if(ID>1){
            File f = new File(path+"checkpoint"+(ID));
            f.mkdir();
            String from =path+"checkpoint"+(ID-1)+"/offsets";
            String to =path+"checkpoint"+(ID)+"/offsets";
            copy(from,to);
        }
        sq =rawData.writeStream()
                .option("path",path+"par"+ID)
                .option("checkpointLocation",path+"checkpoint"+ID)
                .start();


        sq.awaitTermination();



    }

    public void stop(){
        sq.stop();
    }

    @Override
    public void run() {
        try {
            createJob();
        } catch (StreamingQueryException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void copy(String from,String to) throws IOException {
        File sourceLocation= new File(from);
        File targetLocation = new File(to);

        FileUtils.copyDirectory(sourceLocation, targetLocation);
        /*FileInputStream fis = null;
        FileOutputStream fos = null;

        try {

            fis = new FileInputStream(from);
            fos = new FileOutputStream(to);
            int c;
            while ((c = fis.read()) != -1) {
                fos.write(c);
            }
            System.out.println("copied the file successfully");
        }

        catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (fis != null) {
                fis.close();
            }
            if (fos != null) {
                fos.close();
            }
        }*/
    }
}
