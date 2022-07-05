package com.company;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.opencsv.CSVWriter;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.boot.configurationprocessor.json.JSONException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;

public class Main
{
    public static void writeFileToHDFS(FileSystem fileSystem, Path hdfsWritePath, ArrayList<HealthMessage> file, boolean overWrite) throws IOException, ParseException {
        FSDataOutputStream fsDataOutputStream;
        if(overWrite)
            fsDataOutputStream = fileSystem.create(hdfsWritePath);
        else
            fsDataOutputStream = fileSystem.append(hdfsWritePath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        String[] header = { "serviceName", "Timestamp", "CPU", "RAM-Total", "RAM-Free", "Disk-Total", "Disk-Free"};
        CSVWriter writer = new CSVWriter(bufferedWriter);
        //writer.writeNext(header);
        for (int i = 0; i < 120; i++) {
            Path newPath = getPath(file.get(i).getStamp());
            if(!newPath.equals(hdfsWritePath)) {
                hdfsWritePath = newPath;
                fsDataOutputStream = fileSystem.create(hdfsWritePath);
                bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
                writer.close();
                writer = new CSVWriter(bufferedWriter);
            }
            String[] data = {file.get(i).getServiceName(),Long.toString(file.get(i).getStamp()),Double.toString(file.get(i).getCpu()),
                    Double.toString( file.get(i).getRam_Total()),Double.toString(file.get(i).getRam_Free())
                    ,Double.toString(file.get(i).getDisk_Total()),Double.toString(file.get(i).getDisk_Free())};

            writer.writeNext(data);
            if(i == 119)
                System.out.println(Arrays.toString(data));
        }
        bufferedWriter.close();
        fileSystem.close();
    }

    @SneakyThrows
    public static void writeInJson(ArrayList<HealthMessage> file, int i) throws JsonProcessingException {

        ObjectMapper mapper = new ObjectMapper();
        FileWriter f = new FileWriter("/home/hadoopuser/speedlayer/data/output"+ i + ".json");
        int counter = 0;
        for(HealthMessage object : file){
            String json = mapper.writeValueAsString(object);
            f.write(json);
            if(counter != 59)
                f.write("\n");
            counter++;
        }
        f.close();

    }


    public static FileSystem createFileSystem() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        configuration.set("dfs.replication","1");
        FileSystem fileSystem = FileSystem.get(configuration);
        return fileSystem;
    }

    public static Path getPath(){
        LocalDate dayDate = LocalDate.now();
        String fileName = "/" + dayDate + ".csv";
        Path hdfsWritePath = new Path(fileName);
        return hdfsWritePath;
    }
    public static Path getPath(long timestamp) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new Date(timestamp*1000));

        String toGetFolder = date.split(" ")[0];
        String folder = String.valueOf((sdf.parse(toGetFolder+" 00:00:00").getTime()) /1000);

        String file = String.valueOf((sdf.parse(toGetFolder+" " + date.split(" ")[1].split(":")[0] + ":00:00").getTime()) /1000);

        LocalDate dayDate = LocalDate.now();
        String fileName = "/Data/"+ folder + "/" + file + ".csv";
        Path hdfsWritePath = new Path(fileName);
        return hdfsWritePath;
    }

    public static void main(String[] args) throws IOException, JSONException, ParseException {
        DatagramSocket ds = new DatagramSocket(3500);
        byte[] receive = new byte[65535];

        ArrayList<HealthMessage> file = new ArrayList<>();
        ArrayList<HealthMessage> fileJson = new ArrayList<>();
        DatagramPacket DpReceive = null;


//        long startOfRec = System.currentTimeMillis();
//        int counter = 0;
        int i = 1;
        boolean write  = true;
        while (true)
        {
            FileSystem fileSystem = createFileSystem();


            DpReceive = new DatagramPacket(receive, receive.length);

            ds.receive(DpReceive);
            String msg = new String(DpReceive.getData(),0,DpReceive.getLength());

            System.out.println(msg);

            HealthMessage healthMessage = JsonParser.parse(msg);


            file.add(healthMessage);
            fileJson.add(healthMessage);

            if (write){
                String path = "/home/hadoopuser/Downloads/timestamp_mapreduce.txt";
                FileWriter myWriter = new FileWriter(path);
                myWriter.write(String.valueOf(healthMessage.getStamp()));
                myWriter.close();
                write = false;
            }

            System.out.println(fileJson.size());
            if(fileJson.size() % 60 == 0){
                writeInJson(fileJson,i);
                i++;
                fileJson = new ArrayList<>();
            }

            if(file.size() == 120){
                //Create a path
                Path hdfsWritePath = getPath(file.get(0).getStamp());

//                long endOfRec = System.currentTimeMillis();
//                System.out.println("-------------------------------------------------------------------------------------");
//                System.out.println("Throughput of collecting  1024 health message : " + ((1024 * 1.0) / (endOfRec - startOfRec)) + " health message / msec");
//                long startOfWrite = System.currentTimeMillis();

                if(fileSystem.exists(hdfsWritePath)){
                    writeFileToHDFS(fileSystem, hdfsWritePath, file,false);
                }
                else{
                    writeFileToHDFS(fileSystem, hdfsWritePath, file,true);
                }

                System.out.println("=====================");
                System.out.println("=====================");
                System.out.println("=====================");

//                long endOfWrite = System.currentTimeMillis();
//                System.out.println("Throughput of writing  1024 health message : " + ((1024 * 1.0) / (endOfWrite - startOfWrite)) + " health message / msec");
//                counter = 0;
                file = new ArrayList<>();
//                startOfRec = System.currentTimeMillis();
            }

            receive = new byte[65535];
        }
    }
}