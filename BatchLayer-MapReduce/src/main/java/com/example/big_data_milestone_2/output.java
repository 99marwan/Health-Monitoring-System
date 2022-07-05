package com.example.big_data_milestone_2;

public class output {
    private String id ;
    private String ServiceName ;
    private String  CPU ;
    private String  RAM ;
    private String  Disk ;
    private String  MaxCPU ;
    private String  MaxRAM ;
    private String  MaxDisk ;
    private String  Count ;

    private String timeStamp;

    public String getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(String timeStamp) {
        this.timeStamp = timeStamp;
    }



    public String getServiceName() {
        return ServiceName;
    }

    public void setServiceName(String serviceName) {
        ServiceName = serviceName;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCPU() {
        return CPU;
    }

    public void setCPU(String CPU) {
        this.CPU = CPU;
    }

    public String getRAM() {
        return RAM;
    }

    public void setRAM(String RAM) {
        this.RAM = RAM;
    }

    public String getDisk() {
        return Disk;
    }

    public void setDisk(String disk) {
        Disk = disk;
    }

    public String getMaxCPU() {
        return MaxCPU;
    }

    public void setMaxCPU(String maxCPU) {
        MaxCPU = maxCPU;
    }

    public String getMaxRAM() {
        return MaxRAM;
    }

    public void setMaxRAM(String maxRAM) {
        MaxRAM = maxRAM;
    }

    public String getMaxDisk() {
        return MaxDisk;
    }

    public void setMaxDisk(String maxDisk) {
        MaxDisk = maxDisk;
    }

    public String getCount() {
        return Count;
    }

    public void setCount(String count) {
        Count = count;
    }
}
