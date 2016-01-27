package org.wonderbee.elasticsearch.hive;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.wonderbee.elasticsearch.ElasticSearchOutputFormat;
import org.wonderbee.hadoop.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

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
public class ElasticSearchStorageHandler implements HiveStorageHandler, HiveStoragePredicateHandler {

    public static final String ES_INDEX_NAME = "elasticsearch.index.name";
    public static final String ES_BULK_SIZE = "elasticsearch.bulk.size";
    public static final String ES_ID_FIELD_NAME = "elasticsearch.id.field.name";
    public static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    public static final String ES_IS_JSON = "elasticsearch.is_json";
    public static final String ES_REQUEST_SIZE = "elasticsearch.request.size";
    public static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";
    public static final String ES_QUERY_STRING = "elasticsearch.query.string";

    public static final String COMMA = ",";
    public static final String LOCAL_SCHEME = "file://";
    public static final String DEFAULT_BULK = "1000";
    public static final String DEFAULT_ES_CONFIG = "/etc/elasticsearch/elasticsearch.yml";
    public static final String DEFAULT_ES_PLUGINS = "/usr/local/share/elasticsearch/plugins";
    public static final String ES_CONFIG_HDFS_PATH = "/tmp/elasticsearch/elasticsearch.yml";
    public static final String ES_PLUGINS_HDFS_PATH = "/tmp/elasticsearch/plugins";
    public static final String ES_CONFIG = "es.config";
    public static final String ES_PLUGINS = "es.path.plugins";
    public static final String ES_LOCATION = "es.location";
    public static final String ES_HOSTPORT = "es.hostport";

    private static Logger LOG = Logger.getLogger(ElasticSearchStorageHandler.class);
    private Configuration conf;
    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        LOG.info("called getInputFormatClass");
        return ElasticSearchHiveInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        LOG.info("called getOutputFormatClass");
        return ElasticSearchHiveOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        LOG.info("called getSerDeClass");
        return ElasticSearchSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        LOG.info("called getMetaHook");
        return null;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {

    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {

    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        LOG.info("called configureTableJobProperties");
        Properties props = tableDesc.getProperties();
        try {
            // Parse the passed in location URI, pulling out the arguments as well
            String location = props.getProperty(ES_LOCATION);
            String esConfig = props.getProperty(ES_CONFIG);
            String esPlugins = props.getProperty(ES_PLUGINS);
            String esHostPort = props.getProperty(ES_HOSTPORT);

            URI parsedLocation = new URI(location);
            HashMap<String, String> query = parseURIQuery(parsedLocation.getQuery());

            String scheme = "es://";
            if (!location.startsWith(scheme)) {
                scheme = "/";
            }

            String esHost = location.substring(scheme.length()).split("/")[0];
            if (esHost == null) {
                throw new RuntimeException("Missing elasticsearch index name, URI must be formatted as es://<index_name>/<object_type>?<params> or /<index_name>/<object_type>?<params>");
            }

            if (parsedLocation.getPath() == null) {
                throw new RuntimeException("Missing elasticsearch object type, URI must be formatted as es://<index_name>/<object_type>?<params> or /<index_name>/<object_type>?<params>");
            }

            if (conf != null && conf.get(ES_INDEX_NAME) == null) {

                // Set elasticsearch index and object type in the Hadoop configuration
                jobProperties.put(ES_INDEX_NAME, esHost);
                jobProperties.put(ES_OBJECT_TYPE, parsedLocation.getPath().replaceAll(".*/", ""));

                // Set the request size in the Hadoop configuration
                String requestSize = query.get("size");
                if (requestSize == null) requestSize = DEFAULT_BULK;
                jobProperties.put(ES_BULK_SIZE, requestSize);
                jobProperties.put(ES_REQUEST_SIZE, requestSize);

                // Set the id field name in the Hadoop configuration
                String idFieldName = query.get("id");
                if (idFieldName == null) idFieldName = "-1";
                jobProperties.put(ES_ID_FIELD_NAME, idFieldName);

                String queryString = query.get("q");
                if (queryString == null) queryString = "*";
                jobProperties.put(ES_QUERY_STRING, queryString);

                String numTasks = query.get("tasks");
                if (numTasks == null) numTasks = "10";
                jobProperties.put(ES_NUM_SPLITS, numTasks);

                String actionField = query.get("action");
                if (actionField != null) {
                    jobProperties.put(ElasticSearchOutputFormat.ES_ACTION_FIELD, actionField);
                }

                String skipIfExists = query.get("createnew");
                if ("true".equalsIgnoreCase(skipIfExists)) {
                    jobProperties.put(ElasticSearchOutputFormat.ES_SKIP_IF_EXISTS, "true");
                }

                //
                // This gets set even when loading data from elasticsearch
                //
                String isJson = query.get("json");
                if (isJson == null || isJson.equals("false")) {
                    props.setProperty(ES_IS_JSON, "false");
                }

                // Need to set this to start the local instance of elasticsearch
                LOG.info("storage handler set \"es.config\" to " + esConfig);
                jobProperties.put(ES_CONFIG, esConfig);
                conf.set(ES_CONFIG, esConfig);
                LOG.info("storage handler set \"es.path.plugins\" to " + esPlugins);
                jobProperties.put(ES_PLUGINS, esPlugins);
                LOG.info("storage handler set \"es.hostport\" to " + esHostPort);
                jobProperties.put(ES_HOSTPORT, esHostPort);

                // Adds the elasticsearch.yml file (esConfig) and the plugins directory (esPlugins) to the distributed cache
            } else {
                LOG.debug("Initialize called with null conf");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }


        String esConfig = props.getProperty(ES_CONFIG);
        String esPlugins = props.getProperty(ES_PLUGINS);
        try {
            Path hdfsConfigPath = new Path(ES_CONFIG_HDFS_PATH);
            Path hdfsPluginsPath = new Path(ES_PLUGINS_HDFS_PATH);
            LOG.info(LOCAL_SCHEME + esConfig);
            LOG.info(LOCAL_SCHEME + esPlugins);
            HadoopUtils.uploadLocalFile(new Path(LOCAL_SCHEME + esConfig), hdfsConfigPath, conf);
            HadoopUtils.uploadLocalFile(new Path(LOCAL_SCHEME + esPlugins), hdfsPluginsPath, conf);

            HadoopUtils.shipFileIfNotShipped(hdfsConfigPath, conf);
            HadoopUtils.shipArchiveIfNotShipped(hdfsPluginsPath, conf);
            LOG.info("Shipped FILES!");

        } catch (Exception e) {
            throw new RuntimeException("Something went wrong", e);
        }
        LOG.info("final jobProperties "+jobProperties);

    }

    @Override
    public void setConf(Configuration conf) {
        LOG.info("called setConf");
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        LOG.info("called getConf");
        return conf;
    }



    /**
     * Given a URI query string, eg. "foo=bar&happy=true" returns
     * a hashmap ({'foo' => 'bar', 'happy' => 'true'})
     */
    private HashMap<String, String> parseURIQuery(String query) {
        HashMap<String, String> argMap = new HashMap<String, String>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] splitPair = pair.split("=");
                argMap.put(splitPair[0], splitPair[1]);
            }
        }
        return argMap;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf entries, Deserializer deserializer, ExprNodeDesc exprNodeDesc) {
        IndexPredicateAnalyzer analyzer =
                ElasticSearchHiveInputFormat.newIndexPredicateAnalyzer();
        List<IndexSearchCondition> searchConditions =
                new ArrayList<IndexSearchCondition>();
        ExprNodeDesc residualPredicate = analyzer.analyzePredicate(exprNodeDesc, searchConditions);
        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        decomposedPredicate.pushedPredicate = analyzer.translateSearchConditions(
                searchConditions);
        decomposedPredicate.residualPredicate = residualPredicate;
        return decomposedPredicate;
    }
}
