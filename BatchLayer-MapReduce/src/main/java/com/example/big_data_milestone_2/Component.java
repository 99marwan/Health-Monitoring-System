package com.example.big_data_milestone_2;


import com.example.big_data_milestone_2.mapreduce.DoubleArrayWritable;
import com.example.big_data_milestone_2.mapreduce.Mean;
import com.example.big_data_milestone_2.mapreduce.pathFilter;
import com.example.big_data_milestone_2.parquet.ParquetWriter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Timer;
import java.util.TimerTask;


@org.springframework.stereotype.Component
public class Component {

    private static ArrayList<String> readFileFromHDFS(String folderName) throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        FileSystem fileSystem = FileSystem.get(configuration);

        int reduceTasks = 4;

        ArrayList<String> output = new ArrayList<>();
        for (int i = 0; i < reduceTasks; i++) {
            String fileName = "part-r-0000" + i;
            Path hdfsReadPath = new Path("/" + folderName + "/" + fileName);
            FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);

            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            String line = null;
            while ((line=bufferedReader.readLine())!=null){
                System.out.println(line);
                output.add(line);
            }
            inputStream.close();
        }

        // delete existing directory
        Path folder = new Path("/" + folderName);
        if (fileSystem.exists(folder)) {
            fileSystem.delete(folder, true);
        }
        fileSystem.close();

        return output;
    }

    private static void job(String start, String end) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");
        conf.set("start",start);
        conf.set("end",end);
        Job job = Job.getInstance(conf, "statistics");
        job.setJarByClass(Mean.class);
        job.setNumReduceTasks(4);
        job.setMapperClass(Mean.meanMapper.class);
        job.setReducerClass(Mean.meanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleArrayWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.setInputPathFilter(job, pathFilter.class);
        FileInputFormat.addInputPath(job, new Path("/Data"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        job.waitForCompletion(true);

    }

    public static String getResults(String start, String end) throws IOException, InterruptedException, ClassNotFoundException, SQLException {

//        DatagramSocket ds = new DatagramSocket(1000);
//        InetAddress ip = InetAddress.getByName("hadoop-master");
//        byte buf[] = null;
//        String toSend = start  + "," + end;
//        buf = toSend.getBytes();
//
//        DatagramPacket DpSend =
//                new DatagramPacket(buf, buf.length, ip, 5000);
//
//        ds.send(DpSend);

        DuckQuery duckQuery = new DuckQuery();
        ResultSet rs = duckQuery.getQuery(Long.parseLong(start),Long.parseLong(end));
        ArrayList<String> results = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        while (rs.next()) {
            for (int i = 1; i <= 8; i++) {
                stringBuilder.append(rs.getString(i));
                if(i < 8)
                    stringBuilder.append(",");
            }
            results.add(stringBuilder.toString());
            stringBuilder = new StringBuilder();
        }
        rs.close();


        return tokenize(results);
    }

    private static  ArrayList<output> queryOutput(ArrayList<String> tokens) throws JsonProcessingException {
        ArrayList<output> result = new ArrayList<output>();
        int count = 1;
        for(String str : tokens ){
            output temp = new output();

            String[] tes = str.split(",") ;

            temp.setId(String.valueOf(count));
            temp.setServiceName(tes[0]);
            temp.setCPU(tes[1]);
            temp.setRAM(tes[2]);
            temp.setDisk(tes[3]);
            temp.setMaxCPU(tes[4]);
            temp.setMaxRAM(tes[5]);
            temp.setMaxDisk(tes[6]);
            temp.setCount(tes[7]);
            count++ ;
            result.add(temp);
        }

        return result;
    }


    public static void saveInParquet(String start, String end) throws IOException, InterruptedException, ClassNotFoundException {
        job(start ,end);
        ArrayList<String> results = readFileFromHDFS("output");
        ParquetWriter parquetWriter = new ParquetWriter();
        parquetWriter.generateParquetFileFor(Output(results),Long.parseLong(start));
    }



    private static  String tokenize(ArrayList<String> tokens) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        ArrayList<output> result = queryOutput(tokens);
        String mapping = mapper.writeValueAsString(result);

        return mapping;
    }
    private static  ArrayList<output> Output(ArrayList<String> tokens) throws JsonProcessingException {
        ArrayList<output> result = new ArrayList<output>();

        int count = 1 ;

        for(String str : tokens ){
            output temp = new output();

            String[] tes = str.split(",") ;

            temp.setId(String.valueOf(count));
            temp.setServiceName("service-"+count);
            temp.setCPU(tes[1].split("\\[")[1].replaceAll(" ", ""));
            temp.setRAM(tes[2].replaceAll(" ", ""));
            temp.setDisk(tes[3].replaceAll(" ", ""));
            temp.setMaxCPU(tes[4].replaceAll(" ", ""));
            temp.setMaxRAM(tes[5].replaceAll(" ", ""));
            temp.setMaxDisk(tes[6].replaceAll(" ", ""));
            temp.setCount(tes[7].split("\\]")[0].replaceAll(" ", ""));
            temp.setTimeStamp((tes[8].replaceAll(" ", "")).replaceAll("]]",""));
            count++ ;


            result.add(temp);
        }

        return result;
    }
    public static void mapRedTask() throws IOException, InterruptedException, ClassNotFoundException {
        String path = "/home/hadoopuser/Downloads/timestamp_mapreduce.txt";
        File myObj = new File(path);
        Scanner myReader = new Scanner(myObj);
        String data = myReader.nextLine();
        long timestamp = Long.parseLong(data);


        int numOfMinutes = 2;
        for (int i = 0; i < numOfMinutes; i++) {
            saveInParquet(String.valueOf(timestamp),String.valueOf(timestamp+59));
            timestamp += 60;
        }

        FileWriter myWriter = new FileWriter(path);
        myWriter.write(String.valueOf(timestamp));
        myWriter.close();
    }

    public static void givenUsingTimer_whenSchedulingDailyTask_thenCorrect(InetAddress ip,DatagramSocket ds) {
        TimerTask repeatedTask = new TimerTask() {

            @SneakyThrows
            public void run() {

                byte buf[] = null;
                String toSend = "1";
                buf = toSend.getBytes();

                DatagramPacket DpSend =
                        new DatagramPacket(buf, buf.length, ip, 1500);

                ds.send(DpSend);


                mapRedTask();

                Thread.sleep(500);

                toSend = "2";
                buf = toSend.getBytes();

                DpSend =
                        new DatagramPacket(buf, buf.length, ip, 1500);

                ds.send(DpSend);
                System.out.println("send" + toSend);

            }
        };
        Timer timer = new Timer("Timer");

        long delay = 1000L;
        long period = 2010L * 60L;
        timer.scheduleAtFixedRate(repeatedTask, delay, period);
    }
    public  static  void main (String[] args) throws IOException, InterruptedException, ClassNotFoundException, SQLException {
        int portNumber = 7080;

        DatagramSocket ds = new DatagramSocket(portNumber);

        InetAddress ip = InetAddress.getByName("hadoop-master");
        givenUsingTimer_whenSchedulingDailyTask_thenCorrect(ip,ds);

    }


}
