package com.company;

import org.springframework.boot.configurationprocessor.json.JSONException;
import org.springframework.boot.configurationprocessor.json.JSONObject;

public class JsonParser {

    public static  HealthMessage parse (String jsonDataString) throws JSONException {

        //String jsonDataString = "{'serviceName': 'service-4', 'Timestamp': 1647938696, 'CPU': 0.15263343249120898, 'RAM': {'Total': 384.4531580706692, 'Free': 129.14233781802446}, 'Disk': {'Total': 168.3184670093996, 'Free': 111.50671141846638}}";
        HealthMessage Hm = new HealthMessage();
        JSONObject jsonObject = new JSONObject(jsonDataString);


        Hm.setServiceName(jsonObject.getString("serviceName"));
        Hm.setTimestamp(jsonObject.getLong("Timestamp"));
        Hm.setCPU(jsonObject.getDouble("CPU"));

        JSONObject RAM = jsonObject.getJSONObject("RAM");

        RAM ram = new RAM();
        ram.setTotal(RAM.getDouble("Total"));
        ram.setFree(RAM.getDouble("Free"));

        Hm.setRam(ram);

        JSONObject Disk = jsonObject.getJSONObject("Disk");

        Disk disk = new Disk();
        disk.setTotal(Disk.getDouble("Total"));
        disk.setFree(Disk.getDouble("Free"));

        Hm.setDisk(disk);

        return Hm;
    }

}

