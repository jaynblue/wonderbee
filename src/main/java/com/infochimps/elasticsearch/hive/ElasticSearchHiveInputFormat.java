package com.infochimps.elasticsearch.hive;

import com.infochimps.elasticsearch.ElasticSearchSplit;
import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.elasticsearch.action.admin.indices.segments.IndexSegments;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

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
public class ElasticSearchHiveInputFormat implements InputFormat {
    static Log LOG = LogFactory.getLog(ElasticSearchHiveInputFormat.class);
    private Configuration conf = null;

    private Node node;
    private Client client;

    private Integer requestSize;
    private Long numHits;
    private Long numSplits;
    private Long numSplitRecords;
    private String indexName;
    private String objType;
    private String queryString;
    private String hostPort;

    private static final String ES_REQUEST_SIZE = "elasticsearch.request.size";           // number of records to fetch at one time
    private static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";       // number of hadoop map tasks to launch
    private static final String ES_QUERY_STRING = "elasticsearch.query.string";

    private static final String ES_CONFIG_NAME = "elasticsearch.yml";
    private static final String ES_PLUGINS_NAME = "plugins";
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    private static final String ES_CONFIG = "es.config";
    private static final String ES_PLUGINS = "es.path.plugins";
    private static final String ES_HOSTPORT = "es.hostport";
    private static final String SLASH = "/";

    /**
     The number of splits is specified in the Hadoop configuration object.
     */
    public InputSplit[] getSplits(JobConf conf, int j) {
        this.conf = conf;
        this.indexName = conf.get(ES_INDEX_NAME);
        this.objType = conf.get(ES_OBJECT_TYPE);
        this.requestSize = Integer.parseInt(conf.get(ES_REQUEST_SIZE,"100"));
        this.numSplits = Long.parseLong(conf.get(ES_NUM_SPLITS,"10"));
        this.queryString = conf.get(ES_QUERY_STRING,"{\"query\":{\"match_all\":{}}}");

        this.hostPort = conf.get(ElasticSearchStorageHandler.ES_HOSTPORT);
        LOG.info("getSplits called with hostPort " + hostPort);
        //
        // Need to ensure that this is set in the hadoop configuration so we can
        // instantiate a local client. The reason is that no files are in the
        // distributed cache when this is called.
        //


        System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
        System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));

        start_embedded_client();

        initiate_search();

        try {
            IndexSegments segments = this.client.admin().indices().segments(new IndicesSegmentsRequest()).get().getIndices().get(this.indexName);
            for (IndexShardSegments segment : segments) {
                LOG.info(segment.getShardId().getId());
                for (ShardSegments seg : segment.getShards()) {
                    LOG.info(seg.getShardRouting().currentNodeId());
                }
            }

        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits.intValue());

        String tableName = conf.get("mapred.input.dir");
        for(int i = 0; i < numSplits; i++) {
            Long size = (numSplitRecords == 1) ? 1 : numSplitRecords-1;
            splits.add(new HiveInputFormat.HiveInputSplit(new ElasticSearchSplit(queryString, i*numSplitRecords, size, hostPort.split(":")[0],tableName+"/"+i),"ElasticSearchSplit"));
        }
        if (numHits % numSplits > 0) splits.add(new HiveInputFormat.HiveInputSplit(new ElasticSearchSplit(queryString, numSplits*numSplitRecords, numHits % numSplits - 1,hostPort.split(":")[0],tableName+"/"+numSplits),"ElasticSearchSplit"));
        LOG.info("Created ["+splits.size()+"] splits for ["+numHits+"] hits");
        return splits.toArray(new InputSplit[splits.size()]);
    }

    /**
     Starts an embedded elasticsearch client (ie. data = false)
     */
    private void start_embedded_client() {

            LOG.info("Starting transport elasticsearch client ...");
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("client.transport.sniff", true).build();
            String[] split = this.hostPort.split(":");
            String host = split[0];
            int port = Integer.decode(split[1]);

            this.client = new TransportClient()
                    .addTransportAddress(new InetSocketTransportAddress(host, port));
            LOG.info("Transport client started");


    }

    private void initiate_search() {
        SearchResponse response = client.prepareSearch(indexName)
                .setTypes(objType)
                .setSearchType(SearchType.COUNT)
                .setQuery(QueryBuilders.queryString(queryString))
                .setSize(requestSize)
                .execute()
                .actionGet();
        this.numHits = response.hits().totalHits();
        if(numSplits > numHits) numSplits = numHits; // This could be bad
        this.numSplitRecords = (numHits/numSplits);
    }


    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        Thread.dumpStack();
        ElasticSearchRecordReader reader = new ElasticSearchRecordReader();
        LOG.info("getRecordReader called with conf containing hostport " + conf.get(ES_HOSTPORT));
        for (Map.Entry<String,String> entry : conf) {
            LOG.info(entry);
        }
        conf.reloadConfiguration();
        reader.initialize(inputSplit,conf);
        return reader;
    }

    protected class ElasticSearchRecordReader implements RecordReader<Text, Text> {

        private Node node;
        private Client client;

        private String indexName;
        private String objType;
        private Long numSplitRecords;
        private Integer requestSize;
        private Integer recordsRead;
        private Iterator<SearchHit> hitsItr = null;
        private String hostPort;

        private String queryString;
        private Long from;
        private Long recsToRead;

        public ElasticSearchRecordReader() {}

        public void initialize(InputSplit split, JobConf conf) throws IOException {
            this.indexName = conf.get(ES_INDEX_NAME);
            this.objType    = conf.get(ES_OBJECT_TYPE);
            LOG.info("Initializing elasticsearch record reader on index ["+indexName+"] and object type ["+objType+"]");

            //
            // Fetches elasticsearch.yml and the plugins directory from the distributed cache
            //
            try {

                String taskConfigPath = HadoopUtils.fetchFileFromCache(ES_CONFIG_NAME, conf);
                LOG.info("Using ["+taskConfigPath+"] as es.config");
                String taskPluginsPath = HadoopUtils.fetchArchiveFromCache(ES_PLUGINS_NAME, conf);
                LOG.info("Using ["+taskPluginsPath+"] as es.plugins.dir");
                System.setProperty(ES_CONFIG, taskConfigPath);
                System.setProperty(ES_PLUGINS, taskPluginsPath+SLASH+ES_PLUGINS_NAME);



//                String taskConfigPath = null;
//                String taskPluginsPath = null;
//                try {
//                    taskConfigPath = HadoopUtils.fetchFileFromCache(ES_CONFIG_NAME, conf);
//                    taskPluginsPath = HadoopUtils.fetchArchiveFromCache(ES_PLUGINS_NAME, conf);
//                } catch (IOException e) {
//                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
//                }
//                LOG.info("input format got "+taskConfigPath+" for config and "+taskPluginsPath+" for plugins");
//
//
//                System.setProperty(ES_CONFIG,  taskConfigPath);
//                System.setProperty(ES_PLUGINS, taskPluginsPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            HiveInputFormat.HiveInputSplit hiveSplit = (HiveInputFormat.HiveInputSplit)split;
            ElasticSearchSplit esSplit = (ElasticSearchSplit)(hiveSplit.getInputSplit());
            queryString = esSplit.getQueryString();
            from = esSplit.getFrom();
            recsToRead = esSplit.getSize();
            hostPort = conf.get(ES_HOSTPORT);
            LOG.info("elasticsearch record reader: query ["+queryString+"], from ["+from+"], size ["+recsToRead+"]");
            start_embedded_client();
            recordsRead = 0;
        }

        /**
         Starts an embedded elasticsearch client (ie. data = false)
         */
        private void start_embedded_client() {
            if (this.hostPort != null) {
                LOG.info("Starting transport elasticsearch client ...");
                Settings settings = ImmutableSettings.settingsBuilder()
                        .put("client.transport.sniff", true).build();
                String[] split = this.hostPort.split(":");
                String host = split[0];
                int port = Integer.decode(split[1]);

                this.client = new TransportClient()
                        .addTransportAddress(new InetSocketTransportAddress(host, port));
                LOG.info("Transport client started");
            } else {
                LOG.info("Starting embedded elasticsearch client ...");
                this.node   = NodeBuilder.nodeBuilder().client(true).node();
                this.client = node.client();
                LOG.info("Embedded elasticsearch client started");
            }
        }

        private Iterator<SearchHit> fetchNextHits() {
            SearchResponse response = client.prepareSearch(indexName)
                    .setTypes(objType)
                    .setFrom(from.intValue())
                    .setSize(recsToRead.intValue())
                    .setQuery(QueryBuilders.queryString(queryString))
                    .execute()
                    .actionGet();
            return response.hits().iterator();
        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            if (hitsItr!=null) {
                //This should obviously be refactored
                if (recordsRead < recsToRead) {
                    if (hitsItr.hasNext()) {
                        SearchHit hit = hitsItr.next();
                        key.set(hit.id());
                        value.set(hit.sourceAsString());
                        recordsRead += 1;
                        return true;
                    }
                } else {
                    hitsItr = null;
                }
            } else {
                if (recordsRead < recsToRead) {
                    hitsItr = fetchNextHits();
                    if (hitsItr.hasNext()) {
                        SearchHit hit = hitsItr.next();
                        key.set(hit.id());
                        value.set(hit.sourceAsString());
                        recordsRead += 1;
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public Text createKey() {
            return new Text();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

        @Override
        public void close() throws IOException {
            LOG.info("Closing record reader");
            client.close();
            LOG.info("Client is closed");
            if (node != null) {
                node.close();
            }
            LOG.info("Record reader closed.");
        }

        @Override
        public float getProgress() throws IOException {
            return 0;  //To change body of implemented methods use File | Settings | File Templates.
        }

    }
}
