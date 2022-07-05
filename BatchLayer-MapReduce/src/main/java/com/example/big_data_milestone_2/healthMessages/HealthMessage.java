package com.example.big_data_milestone_2.healthMessages;

public class HealthMessage {
    private    String ServiceName ;
    private    long Timestamp ;
    private    Double CPU ;
    private RAM Ram ;
    private Disk disk;

    public RAM getRam() {
        return Ram;
    }

    public void setRam(RAM ram) {
        Ram = ram;
    }

    public Disk getDisk() {
        return disk;
    }

    public void setDisk(Disk disk) {
        this.disk = disk;
    }


    public void setServiceName(String serviceName) {
        ServiceName = serviceName;
    }

    public void setTimestamp(long timestamp) {
        Timestamp = timestamp;
    }

    public void setCPU(Double CPU) {
        this.CPU = CPU;
    }


    public String getServiceName() {
        return ServiceName;
    }

    public long getTimestamp() {
        return Timestamp;
    }

    public Double getCPU() {
        return CPU;
    }
}
