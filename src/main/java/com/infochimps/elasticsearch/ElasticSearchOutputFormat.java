package com.infochimps.elasticsearch;

import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Progressable;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
   
   Hadoop OutputFormat for writing arbitrary MapWritables (essentially HashMaps) into Elasticsearch. Records are batched up and sent
   in a one-hop manner to the elastic search data nodes that will index them.
   
 */
public class ElasticSearchOutputFormat extends OutputFormat<NullWritable, MapWritable> {
    public static final String ES_ACTION_FIELD = "elasticsearch.action.field";
    public static final String ES_SKIP_IF_EXISTS = "es.skip.if.exits";

    static Log LOG = LogFactory.getLog(ElasticSearchOutputFormat.class);
    private Configuration conf = null;

    public class ElasticSearchRecordWriter extends RecordWriter<NullWritable, MapWritable> {

        private Node node;
        private Client client;
        private String indexName;
        private int bulkSize;
        private int idField;
        private String idFieldName;
        private String objType;
        private String[] fieldNames;
        private boolean skipIfExists = false;
        private String hostPort;
        // Used for bookkeeping purposes
        private AtomicLong totalBulkTime  = new AtomicLong();
        private AtomicLong totalBulkItems = new AtomicLong();
        private Random     randgen        = new Random();        
        private long       runStartTime   = System.currentTimeMillis();

        // For hadoop configuration
        private static final String ES_CONFIG_NAME = "elasticsearch.yml";
        private static final String ES_PLUGINS_NAME = "plugins";
        private static final String ES_INDEX_NAME = "elasticsearch.index.name";
        private static final String ES_BULK_SIZE = "elasticsearch.bulk.size";
        private static final String ES_ID_FIELD_NAME = "elasticsearch.id.field.name";
        private static final String ES_ID_FIELD = "elasticsearch.id.field";
        private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
        private static final String ES_CONFIG = "es.config";
        private static final String ES_PLUGINS = "es.path.plugins";



        // Other string constants
        private static final String COMMA = ",";
        private static final String SLASH = "/";
        private static final String NO_ID_FIELD = "-1";
        
        private volatile BulkRequestBuilder currentRequest;
        private String actionField = null;

        /**
           Instantiates a new RecordWriter for Elasticsearch
           <p>
           The properties that <b>MUST</b> be set in the hadoop Configuration object
           are as follows:
           <ul>
           <li><b>elasticsearch.index.name</b> - The name of the elasticsearch index data will be written to. It does not have to exist ahead of time</li>
           <li><b>elasticsearch.bulk.size</b> - The number of records to be accumulated into a bulk request before writing to elasticsearch.</li>
           <li><b>elasticsearch.is_json</b> - A boolean indicating whether the records to be indexed are json records. If false the records are assumed to be tsv, in which case <b>elasticsearch.field.names</b> must be set and contain a comma separated list of field names</li>
           <li><b>elasticsearch.object.type</b> - The type of objects being indexed</li>
           <li><b>elasticsearch.config</b> - The full path the elasticsearch.yml. It is a local path and must exist on all machines in the hadoop cluster.</li>
           <li><b>elasticsearch.plugins.dir</b> - The full path the elasticsearch plugins directory. It is a local path and must exist on all machines in the hadoop cluster.</li>
           </ul>
           <p>
           The following fields depend on whether <b>elasticsearch.is_json</b> is true or false.
           <ul>
           <li><b>elasticsearch.id.field.name</b> - When <b>elasticsearch.is_json</b> is true, this is the name of a field in the json document that contains the document's id. If -1 is used then the document is assumed to have no id and one is assigned to it by elasticsearch.</li>
           <li><b>elasticsearch.field.names</b> - When <b>elasticsearch.is_json</b> is false, this is a comma separated list of field names.</li>
           <li><b>elasticsearch.id.field</b> - When <b>elasticsearch.is_json</b> is false, this is the numeric index of the field to use as the document id. If -1 is used the document is assumed to have no id and one is assigned to it by elasticsearch.</li>
           </ul>           
         */
        public ElasticSearchRecordWriter(TaskAttemptContext context) {
            this(context.getConfiguration());
        }

        public ElasticSearchRecordWriter(Configuration conf) {
            this.indexName = conf.get(ES_INDEX_NAME);
            this.bulkSize = Integer.parseInt(conf.get(ES_BULK_SIZE));
            this.idFieldName = conf.get(ES_ID_FIELD_NAME);
            if (idFieldName != null && idFieldName.equals(NO_ID_FIELD)) {
                LOG.info("Documents will be assigned ids by elasticsearch");
                this.idField = -1;
            } else {
                LOG.info("Using field:["+idFieldName+"] for document ids");
            }
            this.objType    = conf.get(ES_OBJECT_TYPE);
            this.actionField = conf.get(ES_ACTION_FIELD);

            if ("true".equalsIgnoreCase(conf.get(ES_SKIP_IF_EXISTS))) {
                this.skipIfExists = true;
            }

            //
            // Fetches elasticsearch.yml and the plugins directory from the distributed cache
            //
            try {

                this.hostPort = conf.get("es.hostport");
                if (hostPort != null) {
                    LOG.info("Using transport client to load to "+hostPort);
                }
                String taskConfigPath = HadoopUtils.fetchFileFromCache(ES_CONFIG_NAME, conf);
                LOG.info("Using ["+taskConfigPath+"] as es.config");
                String taskPluginsPath = HadoopUtils.fetchArchiveFromCache(ES_PLUGINS_NAME, conf);
                LOG.info("Using [" + taskPluginsPath + "] as es.plugins.dir");
                System.setProperty(ES_CONFIG, taskConfigPath);
                System.setProperty(ES_PLUGINS, taskPluginsPath+SLASH+ES_PLUGINS_NAME);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            start_embedded_client();
            initialize_index(indexName);
            currentRequest = client.prepareBulk();
        }


        public void close(TaskAttemptContext context) throws IOException {
            close();
        }

        /**
           Closes the connection to elasticsearch. Any documents remaining in the bulkRequest object are indexed.
         */
        public void close() throws IOException {
            if (currentRequest.numberOfActions() > 0) {            
                try {
                    executeBulkWrite();
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
            }
            LOG.info("Closing record writer");
            client.close();
            LOG.info("Client is closed");
            if (node != null) {
                 node.close();
            }
            LOG.info("Record writer closed.");
        }

        /*
	public void addToIndex(String key, byte[] data) {
		IndexRequestBuilder builder = this.client.prepareIndex(this.indexName, this.indexType, key);
		builder.setConsistencyLevel(WriteConsistencyLevel.QUORUM);
		builder.setReplicationType(ReplicationType.ASYNC);
		builder.setTimeout(timeValueMillis(RESPONSE_TIMEOUT_MILLIS));
		builder.setSource(data);
		this.indexRequest(builder);
	}

	public void deleteFromIndex(String key) {
		DeleteRequestBuilder builder = this.client.prepareDelete(indexName, indexType, key);
		builder.setConsistencyLevel(WriteConsistencyLevel.QUORUM);
		builder.setReplicationType(ReplicationType.ASYNC);
		builder.execute();
	}

         */

        /**
           Writes a single MapWritable record to the bulkRequest object. Once <b>elasticsearch.bulk.size</b> are accumulated the
           records are written to elasticsearch.
         */
        public void write(NullWritable key, MapWritable fields) throws IOException {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            String action = null;
            if (actionField != null) {
                Writable w = fields.get(actionField);
                if (w instanceof Text) {
                    action = w.toString();
                }

                // do not index actionField
                fields.remove(actionField);
            }

            buildContent(builder, fields);

            if (idField == -1) {
                // Document has no inherent id
                if ("delete".equals(action)) {
                    LOG.info("Delete without id field");
                    return;
                }
                else {
                    currentRequest.add(Requests.indexRequest(indexName).type(objType).source(builder));
                }
            } else {
                try {
                    Text mapKey = new Text(idFieldName);
                    String record_id = fields.get(mapKey).toString();
                    if ("delete".equals(action)) {
                        LOG.info("Deleting id: " + record_id);
                        currentRequest.add(Requests.deleteRequest(indexName).id(record_id).type(objType));
                    }
                    else {
                        IndexRequest request = Requests.indexRequest(indexName).id(record_id).type(objType).create(false).source(builder);
                        // request.opType(IndexRequest.OpType.CREATE);
						request.consistencyLevel(org.elasticsearch.action.WriteConsistencyLevel.QUORUM);
						request.replicationType("sync");
                        currentRequest.add(request);
                    }
                } catch (Exception e) {
                    LOG.warn("Encountered malformed record");
                }
            }
            processBulkIfNeeded();
        }

        /**
           Recursively untangles the MapWritable and writes the fields into elasticsearch's XContentBuilder builder.
         */
        private void buildContent(XContentBuilder builder, Writable value) throws IOException {
            if (value instanceof Text) {
                builder.value(((Text)value).toString());
            } else if (value instanceof LongWritable) {
                builder.value(((LongWritable)value).get());
            } else if (value instanceof IntWritable) {
                builder.value(((IntWritable)value).get());
            } else if (value instanceof DoubleWritable) {
                builder.value(((DoubleWritable)value).get());
            } else if (value instanceof FloatWritable) {
                builder.value(((FloatWritable)value).get());
            } else if (value instanceof BooleanWritable) {
                builder.value(((BooleanWritable)value).get());                
            } else if (value instanceof MapWritable) {
                builder.startObject();
                for (Map.Entry<Writable,Writable> entry : ((MapWritable)value).entrySet()) {
                    if (!(entry.getValue() instanceof NullWritable)) {
                        builder.field(entry.getKey().toString());
                        buildContent(builder, entry.getValue());
                    }                    
                }
                builder.endObject();
            } else if (value instanceof ArrayWritable) {
                builder.startArray();
                Writable[] arrayOfThings = ((ArrayWritable)value).get();
                for (int i = 0; i < arrayOfThings.length; i++) {
                    buildContent(builder, arrayOfThings[i]);
                }
                builder.endArray();
            } 
        }

        /**
           Indexes content to elasticsearch when <b>elasticsearch.bulk.size</b> records have been accumulated.
         */
        private void processBulkIfNeeded() {
            totalBulkItems.incrementAndGet();
            if (currentRequest.numberOfActions() >= bulkSize) {
                try {
                    long startTime        = System.currentTimeMillis();
                    executeBulkWrite();
                    totalBulkTime.addAndGet(System.currentTimeMillis() - startTime);

                    if (randgen.nextDouble() < 0.1) {
                        LOG.info("Indexed [" + totalBulkItems.get() + "] in [" + (totalBulkTime.get()/1000) + "s] of indexing"+"[" + ((System.currentTimeMillis() - runStartTime)/1000) + "s] of wall clock"+" for ["+ (float)(1000.0*totalBulkItems.get())/(System.currentTimeMillis() - runStartTime) + "rec/s]");
                    }
                } catch (Exception e) {
                    LOG.warn("Bulk request failed: " + e.getMessage());
                    throw new RuntimeException(e);
                }
                currentRequest = client.prepareBulk();
            }
        }

        private void executeBulkWrite() {
            BulkResponse response = currentRequest.execute().actionGet();
            if(response.hasFailures()) {
                throw new ElasticSearchException(response.buildFailureMessage());
            }
        }

        private void initialize_index(String indexName) {
            LOG.info("Initializing index");
            try {
                client.admin().indices().prepareCreate(indexName).execute().actionGet();
            } catch (Exception e) {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                    LOG.warn("Index ["+indexName+"] already exists");
                } else {
                    LOG.warn(e);
                }
            }
        }

        //
        // Starts an embedded elasticsearch client (ie. data = false)
        //
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
    }

    public RecordWriter<NullWritable, MapWritable> getRecordWriter(final TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchRecordWriter(context);

    }

    public RecordWriter<NullWritable, MapWritable> getRecordWriter(final JobConf jobConf) throws IOException, InterruptedException {
        return new ElasticSearchRecordWriter(jobConf);

    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // TODO Check if the object exists?
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new ElasticSearchOutputCommitter();
    }
}
