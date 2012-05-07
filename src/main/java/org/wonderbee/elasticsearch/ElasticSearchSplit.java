package org.wonderbee.elasticsearch;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Copyright (c) 2012 klout.com
 *
 * Based on work Copyright (c) Infochimps
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
public class ElasticSearchSplit extends FileSplit implements InputSplit, Writable {

    private long from;
    private long size;
    private String host;
    private String tableLocation;
    private String nodeName;
    private int shard;
    public ElasticSearchSplit() {
        super( new Path("dummy"), 0, 0, new String[] {});
    }

    public ElasticSearchSplit(long from, long size, String host, String nodeName, int shard, String tableLocation) {
        super( new Path("dummy"), 0, 0, new String[] {});
        this.from = from;
        this.size = size;
        this.host = host;
        this.shard = shard;
        this.nodeName = nodeName;
        this.tableLocation = tableLocation;
    }

    public long getFrom() {
        return from;
    }

    public long getSize() {
        return size;
    }

    public String getHost() {
        return this.host;
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public int getShard() {
        return this.shard;
    }

    @Override
    public String[] getLocations() {
        return new String[] {host.split(":")[0]};
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
        shard = in.readInt();
        from = in.readLong();
        size = in.readLong();
        host = Text.readString(in);
        nodeName = Text.readString(in);
        tableLocation = Text.readString(in);
    }



    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(shard);
        out.writeLong(from);
        out.writeLong(size);
        Text.writeString(out,host);
        Text.writeString(out,nodeName);
        Text.writeString(out,tableLocation);
    }
}
