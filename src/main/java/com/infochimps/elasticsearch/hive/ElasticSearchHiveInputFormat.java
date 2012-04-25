package com.infochimps.elasticsearch.hive;

import com.infochimps.elasticsearch.ElasticSearchInputFormat;
import com.infochimps.elasticsearch.ElasticSearchSplit;
import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: tristan
 * Date: 4/24/12
 * Time: 5:01 PM
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchHiveInputFormat implements InputFormat, Configurable {
    static Log LOG = LogFactory.getLog(ElasticSearchInputFormat.class);
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

    public org.apache.hadoop.mapred.RecordReader<Text,Text> createRecordReader(org.apache.hadoop.mapred.InputSplit inputSplit,
                                                                                  org.apache.hadoop.mapred.TaskAttemptContext context) {
        return new ElasticSearchRecordReader();
    }

    public List<InputSplit> getSplits(JobContext context) {
        setConf(context.getConfiguration());
        return getSplits();
    }


    /**
     The number of splits is specified in the Hadoop configuration object.
     */
    public List<InputSplit> getSplits() {

        List<InputSplit> splits = new ArrayList<InputSplit>(numSplits.intValue());
        for(int i = 0; i < numSplits; i++) {
            Long size = (numSplitRecords == 1) ? 1 : numSplitRecords-1;
            splits.add(new ElasticSearchSplit(queryString, i*numSplitRecords, size));
        }
        if (numHits % numSplits > 0) splits.add(new ElasticSearchSplit(queryString, numSplits*numSplitRecords, numHits % numSplits - 1));
        LOG.info("Created ["+splits.size()+"] splits for ["+numHits+"] hits");
        return splits;
    }

    /**
     Sets the configuration object, opens a connection to elasticsearch, and
     initiates the initial search request.
     */
    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
        this.indexName = conf.get(ES_INDEX_NAME);
        this.objType = conf.get(ES_OBJECT_TYPE);
        this.requestSize = Integer.parseInt(conf.get(ES_REQUEST_SIZE));
        this.numSplits = Long.parseLong(conf.get(ES_NUM_SPLITS));
        this.queryString = conf.get(ES_QUERY_STRING);
        this.hostPort = conf.get(ES_HOSTPORT);
        //
        // Need to ensure that this is set in the hadoop configuration so we can
        // instantiate a local client. The reason is that no files are in the
        // distributed cache when this is called.
        //
        System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
        System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));

        start_embedded_client();

        initiate_search();
    }

    @Override
    public Configuration getConf() {
        return conf;
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
    public InputSplit[] getSplits(JobConf entries, int i) throws IOException {
        return new InputSplit[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf entries, Reporter reporter) throws IOException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
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


        private String queryString;
        private Long from;
        private Long recsToRead;

        public ElasticSearchRecordReader() {
        }

        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
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
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            queryString = ((ElasticSearchSplit)split).getQueryString();
            from = ((ElasticSearchSplit)split).getFrom();
            recsToRead = ((ElasticSearchSplit)split).getSize();

            LOG.info("elasticsearch record reader: query ["+queryString+"], from ["+from+"], size ["+recsToRead+"]");
            start_embedded_client();
            recordsRead = 0;
        }

        /**
         Starts an embedded elasticsearch client (ie. data = false)
         */
        private void start_embedded_client() {
            LOG.info("Starting embedded elasticsearch client ...");
            this.node   = NodeBuilder.nodeBuilder().client(true).node();
            this.client = node.client();
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
