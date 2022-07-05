package com.example.big_data_milestone_2.parquet;

import com.example.big_data_milestone_2.output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class ParquetWriter {

    public void generateParquetFileFor(ArrayList<output> out,long timestamp) {
        try {
            Schema schema = parseSchema();

            Path path = getPath(timestamp);
            List<GenericData.Record> recordList = generateRecords(schema,out);

            File file = new File(path.toString());

            List<GenericData.Record> readList = new ArrayList<>();

            if(file.exists()){
                ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(path).build();
                GenericData.Record nextRecord = reader.read();;
                while (nextRecord != null){

                    readList.add(nextRecord);
                    nextRecord = reader.read();
                }
            }


            List<GenericData.Record> records = new ArrayList<>() { { addAll(readList); addAll(recordList); } };;

            try (org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withConf(new Configuration())
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .build()) {

                for (GenericData.Record record : records) {
                    writer.write(record);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }

    private static Schema parseSchema() {
        String schemaJson = "{\"namespace\": \"BidData\"," //Not used in Parquet, can put anything
                + "\"type\": \"record\"," //Must be set as record
                + "\"name\": \"BigDataRecord\"," //Not used in Parquet, can put anything
                + "\"fields\": ["
                + " {\"name\": \"serviceName\",  \"type\": \"string\"}"
                + ", {\"name\": \"cpu\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"ram\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"disk\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"maxCPU\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"maxRAM\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"maxDisk\", \"type\": \"double\"}" //Required field
                + ", {\"name\": \"count\", \"type\": \"int\"}" //Required field
                + ", {\"name\": \"stamp\", \"type\": \"long\"}" //Required field
                + " ]}";

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    private static List<GenericData.Record> generateRecords(Schema schema,ArrayList<output> out) {

        List<GenericData.Record> recordList = new ArrayList<>();

        for (int i = 0; i < out.size(); i++) {
            GenericData.Record record = new GenericData.Record(schema);
            record.put("serviceName",out.get(i).getServiceName());
            record.put("cpu",Double.parseDouble(out.get(i).getCPU()));
            record.put("ram", Double.parseDouble(out.get(i).getRAM()));
            record.put("disk", Double.parseDouble(out.get(i).getDisk()));
            record.put("maxCPU", Double.parseDouble(out.get(i).getMaxCPU()));
            record.put("maxRAM", Double.parseDouble(out.get(i).getMaxRAM()));
            record.put("maxDisk", Double.parseDouble(out.get(i).getMaxDisk()));
            record.put("count", Double.parseDouble(out.get(i).getCount()));
            record.put("stamp", Double.parseDouble(out.get(i).getTimeStamp()));
            recordList.add(record);
        }
        return recordList;
    }

    public static Path getPath(long timestamp) throws ParseException {
        SimpleDateFormat sdf =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String date = sdf.format(new Date(timestamp*1000));

        String toGetFolder = date.split(" ")[0];

        String file = String.valueOf((sdf.parse(toGetFolder+" " + date.split(" ")[1].split(":")[0] + ":00:00").getTime()) /1000);

        String fileName = "/home/hadoopuser/Downloads/parquet/"+ file + ".parquet";
        Path path = new Path(fileName);
        return path;
    }
}