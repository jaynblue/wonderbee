package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

public class ElasticSearchSplit extends FileSplit implements InputSplit, Writable {

    private String queryString;
    private long from;
    private long size;
    private String host;
    private String tableLocation;

    public ElasticSearchSplit() {
        super( new Path("dummy"), 0, 0, new String[] {});
    }

    public ElasticSearchSplit(String queryString, long from, long size, String host, String tableLocation) {
        super( new Path("dummy"), 0, 0, new String[] {});
        this.queryString = queryString;
        this.from = from;
        this.size = size;
        this.host = host;
        this.tableLocation = tableLocation;
    }

    public String getQueryString() {
        return queryString;
    }

    public long getFrom() {
        return from;
    }

    public long getSize() {
        return size;
    }
    
    @Override
    public String[] getLocations() {
        return new String[] {host};
    }
    
    @Override
    public long getLength() {
        return 0;
    }

    @Override
    public Path getPath() {
        return new Path(tableLocation);
    }


    @Override
    public void readFields(DataInput in) throws IOException {
        //super.readFields(in);
        queryString = Text.readString(in);
        from = in.readLong();
        size = in.readLong();
        host = Text.readString(in);
        tableLocation = Text.readString(in);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        //super.write(out);
        Text.writeString(out, queryString);
        out.writeLong(from);
        out.writeLong(size);
        Text.writeString(out,host);
        Text.writeString(out,tableLocation);
    }
}
