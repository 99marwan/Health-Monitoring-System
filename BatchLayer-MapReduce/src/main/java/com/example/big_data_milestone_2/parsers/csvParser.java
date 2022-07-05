package com.example.big_data_milestone_2.parsers;


import com.example.big_data_milestone_2.healthMessages.Disk;
import com.example.big_data_milestone_2.healthMessages.HealthMessage;
import com.example.big_data_milestone_2.healthMessages.RAM;
import com.opencsv.CSVReader;


import java.io.IOException;
import java.io.StringReader;

public class csvParser {
    public static HealthMessage parse (String csvData) throws IOException {
        CSVReader R = new CSVReader(new StringReader(csvData));
        String[] data = R.readNext();
        R.close();

        HealthMessage Hm = new HealthMessage();

        Hm.setServiceName(data[0]);

        Hm.setTimestamp(Long.parseLong(data[1]));

        Hm.setCPU(Double.parseDouble(data[2]));



        RAM ram = new RAM();
        ram.setTotal(Double.parseDouble(data[3]));
        ram.setFree(Double.parseDouble(data[4]));

        Hm.setRam(ram);


        Disk disk = new Disk();
        disk.setTotal(Double.parseDouble(data[5]));
        disk.setFree(Double.parseDouble(data[6]));

        Hm.setDisk(disk);

        return Hm;
    }
}
