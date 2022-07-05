package com.company;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Scanner;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.Text;
import org.springframework.boot.configurationprocessor.json.JSONException;

public class Main
{
    public static void main(String args[]) throws IOException, InterruptedException, JSONException {
        Scanner sc = new Scanner(System.in);
        int portNumber = 9080;

        DatagramSocket ds = new DatagramSocket(portNumber);

        InetAddress ip = InetAddress.getByName("hadoop-master");

        byte buf[] = null;

        File folder = new File("/home/hadoopuser/Downloads/Data");
        File[] listOfFiles = folder.listFiles();
        Arrays.sort(listOfFiles);
        System.out.println(Arrays.toString(listOfFiles));

        for (int i = 1; i < listOfFiles.length; i++) {
            int counter = 0;

            String jsonData = new String();
            try {
                File myObj = new File(folder.getPath() + "/" + listOfFiles[i].getName());
                Scanner myReader = new Scanner(myObj);
                jsonData = myReader.nextLine();
                myReader.close();
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }

            String[] jsonObjects = jsonData.split("}}");
            for (String json : jsonObjects) {

                String toSend = json + "}}";
                buf = toSend.getBytes();

                DatagramPacket DpSend =
                        new DatagramPacket(buf, buf.length, ip, 3500);


                ds.send(DpSend);

                Thread.sleep(1000);
            }
            break;

        }
    }
}