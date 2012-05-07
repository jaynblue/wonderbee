package org.wonderbee.elasticsearch.hive;

import org.wonderbee.elasticsearch.ElasticSearchSplit;
import org.wonderbee.hadoop.util.HadoopUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
/**
 * Copyright (c) 2012 klout.com
 *
 * Based in part on work Copyright (c) Infochimps
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
    private String indexName;

    private String hostPort;



    private static final String ES_REQUEST_SIZE = "elasticsearch.request.size";           // number of records to fetch at one time
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
    public InputSplit[] getSplits(JobConf conf, int numSplitsHint) throws IOException {
        this.conf = conf;
        this.indexName = conf.get(ES_INDEX_NAME);
        this.requestSize = Integer.parseInt(conf.get(ES_REQUEST_SIZE, "1000"));

        this.hostPort = conf.get(ElasticSearchStorageHandler.ES_HOSTPORT);


        System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
        System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));

        start_embedded_client();
        LOG.info("Admin client started");
        //Get the mapping of shards to node/hosts with primary
        ClusterState clusterState = client.admin().cluster().prepareState().execute().actionGet().state();
        Map<Integer,String[]> shardToHost = new LinkedHashMap<Integer,String[]>();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable.getAssignedShards()) {
                    if (shardRouting.shardId().index().getName().equals(this.indexName) && shardRouting.primary()) {
                        InetSocketTransportAddress address = (InetSocketTransportAddress) clusterState.nodes().get(shardRouting.currentNodeId()).getAddress();
                        int shardId = shardRouting.shardId().getId();
                        String hostPort = address.address().getHostName()+":"+address.address().getPort();
                        String nodeName = shardRouting.currentNodeId();
                        shardToHost.put(shardId,new String[]{hostPort,nodeName});
                    }
                }
            }
        }
        this.client.close();
        LOG.info("Admin client closed");
        List<InputSplit> splits = new ArrayList<InputSplit>(shardToHost.size());

        Job job = new Job(conf);
        JobContext jobContext = new JobContext(job.getConfiguration(), job.getJobID());
        Path[] tablePaths = FileInputFormat.getInputPaths(jobContext);

        //Consultation with kimchy revealed it should be more efficient to just have as many splits
        //as shards.
        for (Map.Entry<Integer, String[]> pair : shardToHost.entrySet()) {
            int shard = pair.getKey();
            String shardHostPort = pair.getValue()[0];
            String nodeName = pair.getValue()[1];
            LOG.debug("Created split: shard:" + shard + ", host:" + shardHostPort + ", node:" + nodeName);

            splits.add(new HiveInputFormat.HiveInputSplit(new ElasticSearchSplit(0, this.requestSize, shardHostPort, nodeName, shard, tablePaths[0].toString()), "ElasticSearchSplit"));
        }
        return splits.toArray(new InputSplit[splits.size()]);
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


    public static QueryBuilder getPushedDownPredicateQueryBuilder(JobConf jobConf) {
    String filterExprSerialized =
            jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
        return QueryBuilders.matchAllQuery();
    }
    ExprNodeDesc filterExpr =
            Utilities.deserializeExpression(filterExprSerialized, jobConf);
        IndexPredicateAnalyzer analyzer =
                newIndexPredicateAnalyzer();

        List<IndexSearchCondition> searchConditions =
                new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate =
                analyzer.analyzePredicate(filterExpr, searchConditions);
        BoolQueryBuilder bqb = boolQuery();

        for ( IndexSearchCondition condition : searchConditions ) {
            if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan")) {
                bqb.must(rangeQuery(condition.getColumnDesc().getColumn()).gte(condition.getConstantDesc().getValue()));
            } else if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan")) {
                bqb.must(rangeQuery(condition.getColumnDesc().getColumn()).gt(condition.getConstantDesc().getValue()));
            } else if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan")) {
                bqb.must(rangeQuery(condition.getColumnDesc().getColumn()).lte(condition.getConstantDesc().getValue()));
            } else if (condition.getComparisonOp().equals("org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan")) {
                bqb.must(rangeQuery(condition.getColumnDesc().getColumn()).lt(condition.getConstantDesc().getValue()));
            }
        }
        return bqb;
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        ElasticSearchRecordReader reader = new ElasticSearchRecordReader();
        LOG.info("getRecordReader called with conf containing hostport " + conf.get(ES_HOSTPORT));
        conf.reloadConfiguration();
        reader.initialize(inputSplit,conf);
        return reader;
    }

    protected class ElasticSearchRecordReader implements RecordReader<Text, Text> {

        private Node node;
        private Client client;

        private String indexName;
        private String objType;
        private Iterator<SearchHit> hitsItr = null;
        private String hostPort;
        private String nodeName;
        private int shard;
        private QueryBuilder queryBuilder;
        private Long recsToRead;

        private SearchResponse scrollResp;

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

                if (taskConfigPath == null || taskPluginsPath == null ) {
                    //Local
                    System.setProperty(ES_CONFIG, conf.get(ES_CONFIG));
                    System.setProperty(ES_PLUGINS, conf.get(ES_PLUGINS));
                } else {
                    //Distributed
                    System.setProperty(ES_CONFIG, taskConfigPath);
                    System.setProperty(ES_PLUGINS, taskPluginsPath+SLASH+ES_PLUGINS_NAME);
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            HiveInputFormat.HiveInputSplit hiveSplit = (HiveInputFormat.HiveInputSplit)split;
            ElasticSearchSplit esSplit = (ElasticSearchSplit)(hiveSplit.getInputSplit());
            queryBuilder = getPushedDownPredicateQueryBuilder(conf);
            recsToRead = esSplit.getSize();
            hostPort = esSplit.getHost();
            nodeName = esSplit.getNodeName();
            shard = esSplit.getShard();
            LOG.info("elasticsearch record reader: query ["+queryBuilder.toString()+"]");
            start_embedded_client();
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
            LOG.info(String.format("_shards:%s;_prefer_node:%s",shard,nodeName));
            if (this.scrollResp == null) {
                LOG.info("intial scan request");
                this.scrollResp = client.prepareSearch(indexName)
                    .setTypes(objType)
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(600000))
                    .setQuery(queryBuilder)
                    .setPreference(String.format("_shards:%s;_prefer_node:%s",shard,nodeName))
                    .setSize(recsToRead.intValue()).execute().actionGet();
                //SCAN mode only returns hits on the second call
                LOG.info("Number of hits on this shard: "+this.scrollResp.getHits().totalHits());
                if (this.scrollResp.getHits().totalHits() > 0) {
                    this.scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                }
            } else {
                this.scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();

            }
            Iterator<SearchHit> resultHits = scrollResp.hits().iterator();
            LOG.info(resultHits.hasNext());
            return resultHits;


        }

        @Override
        public boolean next(Text key, Text value) throws IOException {
            int nohits = 0;
            //first nohits because iterator ran out of hits
            //second because the next iterator was empty
            while (nohits < 2) {
                if (hitsItr == null) {
                    hitsItr = fetchNextHits();
                }
                if (hitsItr.hasNext()) {
                    SearchHit hit = hitsItr.next();
                    key.set(hit.id());
                    value.set(hit.sourceAsString());
                    return true;
                } else {
                    nohits += 1;
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

    /**
     * Instantiates a new predicate analyzer suitable for
     * determining how to push a filter down into the elasticsearch query,
     * based on the rules for what kinds of pushdown we currently support.
     *
     * @return preconfigured predicate analyzer
     */
    static IndexPredicateAnalyzer newIndexPredicateAnalyzer() {

        IndexPredicateAnalyzer analyzer = new IndexPredicateAnalyzer();

        // for now, we only support greater than and less than since equality
        // is only true for non analyzed fields
        //analyzer.addComparisonOp(
        //        "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual");
        analyzer.addComparisonOp(
                "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan");
        analyzer.addComparisonOp(
                "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan");
        analyzer.addComparisonOp(
                "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan");
        analyzer.addComparisonOp(
                "org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan");

        return analyzer;
    }
}
