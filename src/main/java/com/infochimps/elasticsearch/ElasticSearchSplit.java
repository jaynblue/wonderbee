package com.infochimps.elasticsearch;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputSplit;

public class ElasticSearchSplit implements InputSplit, Writable {

    private String queryString;
    private long from;
    private long size;
    private String host;

    public ElasticSearchSplit() {}

    public ElasticSearchSplit(String queryString, long from, long size) {
        new ElasticSearchSplit(queryString, from, size, "none");
    }

    public ElasticSearchSplit(String queryString, long from, long size, String host) {
        this.queryString = queryString;
        this.from = from;
        this.size = size;
        this.host = host;
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
    public void readFields(DataInput in) throws IOException {
        queryString = Text.readString(in);
        from = in.readLong();
        size = in.readLong();
        host = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, queryString);
        out.writeLong(from);
        out.writeLong(size);
        Text.writeString(out,host);
    }
}
