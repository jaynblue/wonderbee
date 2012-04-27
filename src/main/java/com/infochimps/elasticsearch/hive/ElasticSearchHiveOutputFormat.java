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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;
import org.apache.pig.builtin.LOG;

import java.io.IOException;
import java.util.Properties;

/**
 * Copyright (c) 2012 klout.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ElasticSearchHiveOutputFormat implements HiveOutputFormat<WritableComparable, Writable>, OutputFormat {
    private  ElasticSearchOutputFormat enclosedOutputFormat = new ElasticSearchOutputFormat();
    private static Logger LOG = Logger.getLogger(ElasticSearchHiveOutputFormat.class);
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
        //final FileSystem fs = path.getFileSystem(jobConf);
        return new FileSinkOperator.RecordWriter(){
            public void write(Writable obj) throws IOException {
                esWriter.write(NullWritable.get(),(MapWritable) obj);
            }
            public void close(boolean abort) throws IOException {
                esWriter.close();
            }
        };

    }

    @Override
    public void checkOutputSpecs(FileSystem fileSystem, JobConf entries) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public org.apache.hadoop.mapred.RecordWriter getRecordWriter(
            FileSystem fileSystem,
            JobConf jobConf,
            String name,
            Progressable progressable) throws IOException {
        throw new RuntimeException("Error: Hive should not invoke this method.");
    }

}
