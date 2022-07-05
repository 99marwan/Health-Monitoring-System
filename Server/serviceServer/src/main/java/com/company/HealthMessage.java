package com.company;

import org.codehaus.jackson.annotate.JsonPropertyOrder;
@JsonPropertyOrder({"serviceName","stamp","cpu","ram_Total","ram_Free","disk_Total","disk_Free"})
public class HealthMessage {
    private    String serviceName;
    private    long stamp;
    private    Double cpu;

    private    Double ram_Total;
    private    Double ram_Free;

    private    Double disk_Total;
    private    Double disk_Free;



    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public void setStamp(long stamp) {
        this.stamp = stamp;
    }

    public void setCpu(Double cpu) {
        this.cpu = cpu;
    }


    public String getServiceName() {
        return serviceName;
    }

    public long getStamp() {
        return stamp;
    }

    public Double getCpu() {
        return cpu;
    }

    public Double getRam_Total() {
        return ram_Total;
    }

    public void setRam_Total(Double ram_Total) {
        this.ram_Total = ram_Total;
    }

    public Double getRam_Free() {
        return ram_Free;
    }

    public void setRam_Free(Double ram_Free) {
        this.ram_Free = ram_Free;
    }


    public Double getDisk_Total() {
        return disk_Total;
    }

    public void setDisk_Total(Double disk_Total) {
        this.disk_Total = disk_Total;
    }

    public Double getDisk_Free() {
        return disk_Free;
    }

    public void setDisk_Free(Double disk_Free) {
        this.disk_Free = disk_Free;
    }
}
