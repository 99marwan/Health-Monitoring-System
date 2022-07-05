package com.example.big_data_milestone_2.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class DoubleArrayWritable extends ArrayWritable {

    //Instantiate DoubleArrayWritable using DoubleWritable.class
    public DoubleArrayWritable() {
        super(DoubleWritable.class);
    }
    public DoubleArrayWritable(Double[] doubles) {
        super(DoubleWritable.class);
        DoubleWritable[] texts = new DoubleWritable[doubles.length];
        for (int i = 0; i < doubles.length; i++) {
            texts[i] = new DoubleWritable(doubles[i]);
        }
        set(texts);
    }

    //Return the values held within the DoubleArrayWritable class
    public double[] getValueArray(){
        Writable[] writableValues = get();
        double[] valueArray = new double[writableValues.length];
        for (int i = 0 ; i < valueArray.length; i++){
            valueArray[i] = ((DoubleWritable)(writableValues[i])).get();
        }
        return valueArray;
    }


}