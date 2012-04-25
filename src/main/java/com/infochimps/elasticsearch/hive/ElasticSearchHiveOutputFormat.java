package com.infochimps.elasticsearch.hive;

import com.infochimps.elasticsearch.ElasticSearchOutputFormat;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.util.Properties;

/**
 * Created with IntelliJ IDEA.
 * User: tristan
 * Date: 4/24/12
 * Time: 2:06 PM
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchHiveOutputFormat implements HiveOutputFormat<NullWritable,MapWritable> {
    private  ElasticSearchOutputFormat enclosedOutputFormat = new ElasticSearchOutputFormat();

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(final JobConf jobConf, final Path path,
                                                             final Class<? extends Writable> aClass,
                                                             boolean b, final Properties tableProperties,
                                                             final Progressable progressable) throws IOException {
        final ElasticSearchOutputFormat.ElasticSearchRecordWriter esWriter;
        try {
            esWriter = (ElasticSearchOutputFormat.ElasticSearchRecordWriter) enclosedOutputFormat.getRecordWriter(jobConf);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        final FileSystem fs = path.getFileSystem(jobConf);
        return new FileSinkOperator.RecordWriter(){
            public void write(Writable obj) throws IOException {
                esWriter.write(NullWritable.get(),(MapWritable) obj);
            }
            public void close(boolean bool) throws IOException {
                esWriter.close();
                //FileSinkOperator will throw an exception if we don't write anything.
                fs.create(path).close();
            }
        };

    }
}
