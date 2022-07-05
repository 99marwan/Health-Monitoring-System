package com.example.big_data_milestone_2.mapreduce;

import com.example.big_data_milestone_2.healthMessages.HealthMessage;
import com.example.big_data_milestone_2.parsers.csvParser;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;


public class Mean {

    public static long start;
    public static long end;

    public static double getMinTimeStamp(long timestamp) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new Date(timestamp*1000));

        String toGetFolder = date.split(" ")[0];

        String file = String.valueOf((sdf.parse(toGetFolder+" " + date.split(" ")[1].split(":")[0] +  ": "+  date.split(" ")[1].split(":")[1]  +":00").getTime()) /1000);


        return (double)Long.parseLong(file);
    }


    public static class meanMapper
            extends Mapper<Object, Text, Text, ArrayWritable> {

        private Text serviceName = new Text();
        private double cpu = 0;
        private double ram = 0;
        private double disk = 0;
        private double timestamp = 0;

        @SneakyThrows
        public void map(Object key, Text value, Context context){

            Configuration conf = context.getConfiguration();
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();

            long start = Long.parseLong(conf.get("start"));
            long end = Long.parseLong(conf.get("end"));


                String Data = value.toString();

                HealthMessage healthMessage = csvParser.parse(Data);
                timestamp = healthMessage.getTimestamp();

                if (timestamp >= start && timestamp <= end) {
                    cpu = (healthMessage.getCPU());
                    ram = (healthMessage.getRam().getTotal() - healthMessage.getRam().getFree());
                    disk = (healthMessage.getDisk().getTotal() - healthMessage.getDisk().getFree());
                    serviceName.set(healthMessage.getServiceName());

                    Double[] doubles = new Double[4];
                    doubles[0] = cpu;
                    doubles[1] = ram;
                    doubles[2] = disk;
                    doubles[3] = timestamp;
                    context.write(serviceName, new DoubleArrayWritable(doubles));
                }


        }
    }

    public static class meanReducer
            extends Reducer<Text, DoubleArrayWritable, Text, DoubleArrayWritable> {

        @SneakyThrows
        public void reduce(Text key, Iterable<DoubleArrayWritable> values,
                           Context context) throws IOException, InterruptedException {

            double maxUtilCPU = 0;
            double maxUtilRAM = 0;
            double maxUtilDISK = 0;

            double timestampCPU = 0;
            double timestampRAM = 0;
            double timestampDISK = 0;

            double sumcp = 0;
            double sumRam = 0;
            double sumDisk = 0;

            double counter = 0;
            double timeStamp =0;

            double[] result;
            for (DoubleArrayWritable val : values) {
                result = val.getValueArray();
                sumcp += result[0];
                sumRam += result[1];
                sumDisk += result[2];
                timeStamp = result[3];
                if(result[0] > maxUtilCPU){
                    maxUtilCPU = result[0];
                    timestampCPU = result[3];
                }
                if(result[1] > maxUtilRAM){
                    maxUtilRAM = result[1];
                    timestampRAM = result[3];
                }
                if(result[2] > maxUtilDISK){
                    maxUtilDISK = result[2];
                    timestampDISK = result[3];
                }
                counter ++;
            }

            sumcp /= counter;
            sumRam /= counter;
            sumDisk /= counter;

            Double[] doubles = new Double[8];
            doubles[0] = sumcp;
            doubles[1] = sumRam;
            doubles[2] = sumDisk;
            doubles[3] = timestampCPU;
            doubles[4] = timestampRAM;
            doubles[5] = timestampDISK;
            doubles[6] = counter;
            doubles[7] = getMinTimeStamp((long)(timeStamp));
            //System.err.println((doubles[7]));
            context.write(key, new DoubleArrayWritable(doubles));
        }

    }

}