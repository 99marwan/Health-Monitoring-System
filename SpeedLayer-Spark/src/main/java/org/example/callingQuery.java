package org.example;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.sql.SQLException;

public class callingQuery implements Runnable{
    int i;
    boolean schedule;
    public callingQuery(int i, boolean schedule) {
        this.i = i;
        this.schedule = schedule;
    }


    @Override
    public void run() {
        //wait message on backend
        DatagramSocket ds = null;
        DuckQuery duckQuery = new DuckQuery();
        try {
            System.out.println("here");
            ds = new DatagramSocket(5000);
            byte[] receive = new byte[65535];
            DatagramPacket DpReceive  = null;

            while (true) {
                DpReceive = new DatagramPacket(receive, receive.length);
                ds.receive(DpReceive);
                String msg = new String(DpReceive.getData(), 0, DpReceive.getLength());
                System.out.println(msg);

                String[] startEnd = msg.split(",");


                    if (schedule){
                        duckQuery.getQuery(i-2,Long.parseLong(startEnd[0]),Long.parseLong(startEnd[1]));
                    }
                    else {
                        duckQuery.getQuery(i-1,Long.parseLong(startEnd[0]),Long.parseLong(startEnd[1]));
                    }


            }
        } catch (IOException | SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }



    }
}
