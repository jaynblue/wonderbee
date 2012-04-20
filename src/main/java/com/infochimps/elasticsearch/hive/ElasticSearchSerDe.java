package com.infochimps.elasticsearch.hive;

import com.infochimps.elasticsearch.ElasticSearchOutputFormat;
import com.infochimps.elasticsearch.hadoop.util.HadoopUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: tristan
 * Date: 4/6/12
 * Time: 3:44 PM
 * To change this template use File | Settings | File Templates.
 */
public class ElasticSearchSerDe implements SerDe {

    private static final Logger LOG = Logger.getLogger(ElasticSearchSerDe.class);
    private int numColumns;
    private StructObjectInspector rowOI;
    private List<String> columnNames;
    private List<TypeInfo> columnTypes;
    private Properties props;
    protected ObjectMapper mapper = new ObjectMapper();

    // For hadoop configuration
    private static final String ES_INDEX_NAME = "elasticsearch.index.name";
    private static final String ES_BULK_SIZE = "elasticsearch.bulk.size";
    private static final String ES_ID_FIELD_NAME = "elasticsearch.id.field.name";
    private static final String ES_OBJECT_TYPE = "elasticsearch.object.type";
    private static final String ES_IS_JSON = "elasticsearch.is_json";
    private static final String PIG_ES_FIELD_NAMES = "elasticsearch.pig.field.names";
    private static final String ES_REQUEST_SIZE = "elasticsearch.request.size";
    private static final String ES_NUM_SPLITS = "elasticsearch.num.input.splits";
    private static final String ES_QUERY_STRING = "elasticsearch.query.string";

    private static final String COMMA = ",";
    private static final String LOCAL_SCHEME = "file://";
    private static final String DEFAULT_BULK = "1000";
    private static final String DEFAULT_ES_CONFIG = "/etc/elasticsearch/elasticsearch.yml";
    private static final String DEFAULT_ES_PLUGINS = "/usr/local/share/elasticsearch/plugins";
    private static final String ES_CONFIG_HDFS_PATH = "/tmp/elasticsearch/elasticsearch.yml";
    private static final String ES_PLUGINS_HDFS_PATH = "/tmp/elasticsearch/plugins";
    private static final String ES_CONFIG = "es.config";
    private static final String ES_PLUGINS = "es.path.plugins";
    private static final String ES_LOCATION = "es.location";
    private static final String ES_HOSTPORT = "es.hostport";


    @Override
    public void initialize(Configuration conf, Properties properties) throws SerDeException {
        try {
            props = properties;
            // We can get the table definition from tbl.
            String columnNameProperty = props.getProperty(Constants.LIST_COLUMNS);
            String columnTypeProperty = props.getProperty(Constants.LIST_COLUMN_TYPES);
            columnNames = Arrays.asList(columnNameProperty.split(","));
            columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
            LOG.info(columnNames);
            LOG.info(columnTypes);
            assert columnNames.size() == columnTypes.size();
            numColumns = columnNames.size();

            //Build the object inspector based on the types of the columns.  Maybe should just be text?
            List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
            for (int c = 0; c < numColumns; c++) {
                try {
                    columnOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(((PrimitiveTypeInfo) columnTypes.get(c)).getPrimitiveCategory()));
                } catch (ClassCastException classCast) {
                    columnOIs.add(ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector));
                }
            }
            LOG.info(columnOIs);
            rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

            // Parse the passed in location URI, pulling out the arguments as well
            String location = properties.getProperty(ES_LOCATION);
            //LOG.info(location);
            String esConfig = properties.getProperty(ES_CONFIG);
            //LOG.info(esConfig);
            String esPlugins = properties.getProperty(ES_PLUGINS);
            //LOG.info(esPlugins);
            String esHostPort = properties.getProperty(ES_HOSTPORT);
            LOG.info(esHostPort);
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


            // Adds the elasticsearch.yml file (esConfig) and the plugins directory (esPlugins) to the distributed cache
            try {
                Configuration c = new Configuration();
                Path hdfsConfigPath = new Path(ES_CONFIG_HDFS_PATH);
                Path hdfsPluginsPath = new Path(ES_PLUGINS_HDFS_PATH);

                HadoopUtils.uploadLocalFileIfChanged(new Path(LOCAL_SCHEME + esConfig), hdfsConfigPath, c);
                LOG.info(hdfsConfigPath);
                LOG.info(new Path(LOCAL_SCHEME + esConfig));
                HadoopUtils.shipFileIfNotShipped(hdfsConfigPath, c);

                HadoopUtils.uploadLocalFileIfChanged(new Path(LOCAL_SCHEME + esPlugins), hdfsPluginsPath, c);
                HadoopUtils.shipArchiveIfNotShipped(hdfsPluginsPath, c);

            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            if (conf != null && conf.get(ES_INDEX_NAME) == null) {

                // Set elasticsearch index and object type in the Hadoop configuration
                conf.set(ES_INDEX_NAME, esHost);
                conf.set(ES_OBJECT_TYPE, parsedLocation.getPath().replaceAll(".*/", ""));

                // Set the request size in the Hadoop configuration
                String requestSize = query.get("size");
                if (requestSize == null) requestSize = DEFAULT_BULK;
                conf.set(ES_BULK_SIZE, requestSize);
                conf.set(ES_REQUEST_SIZE, requestSize);

                // Set the id field name in the Hadoop configuration
                String idFieldName = query.get("id");
                if (idFieldName == null) idFieldName = "-1";
                conf.set(ES_ID_FIELD_NAME, idFieldName);

                String queryString = query.get("q");
                if (queryString == null) queryString = "*";
                conf.set(ES_QUERY_STRING, queryString);

                String numTasks = query.get("tasks");
                if (numTasks == null) numTasks = "100";
                conf.set(ES_NUM_SPLITS, numTasks);

                String actionField = query.get("action");
                if (actionField != null) {
                    conf.set(ElasticSearchOutputFormat.ES_ACTION_FIELD, actionField);
                }

                String skipIfExists = query.get("createnew");
                if ("true".equalsIgnoreCase(skipIfExists)) {
                    conf.set(ElasticSearchOutputFormat.ES_SKIP_IF_EXISTS, "true");
                }

                //
                // This gets set even when loading data from elasticsearch
                //
                String isJson = query.get("json");
                if (isJson == null || isJson.equals("false")) {
                    props.setProperty(ES_IS_JSON, "false");
                }

                // Need to set this to start the local instance of elasticsearch
                conf.set(ES_CONFIG, esConfig);
                conf.set(ES_PLUGINS, esPlugins);
                conf.set(ES_HOSTPORT, esHostPort);
            } else {
                LOG.info("Initialize called with null conf!");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        List<? extends StructField> outputFieldRefs = outputRowOI
                .getAllStructFieldRefs();
        MapWritable record = new MapWritable();

        String isJson = props.getProperty(ES_IS_JSON);
        // Handle delimited records (ie. isJson == false)

        for (int c = 0; c < numColumns; c++) {
            try {
                Object field = outputRowOI.getStructFieldData(obj,
                        outputFieldRefs.get(c));
                ObjectInspector fieldOI = outputFieldRefs.get(c)
                        .getFieldObjectInspector();

                PrimitiveObjectInspector fieldStringOI = (PrimitiveObjectInspector) fieldOI;
                String columnName = columnNames.get(c);
                record.put(new Text(columnName), (Writable) fieldStringOI.getPrimitiveWritableObject(field));
            } catch (NullPointerException e) {
                //LOG.info("Increment null field counter.");
            }

        }


        return record;
    }

    @Override
    public Object deserialize(Writable writable) throws SerDeException {
        throw new SerDeException("Deserialize not yet supported!");  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        LOG.info("Returning ObjectInspector");
        LOG.info(rowOI);
        return rowOI;
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

    /**
     * Recursively converts an arbitrary object into the appropriate writable. Please enlighten me if there is an existing
     * method for doing this.
     */
    private Writable toWritable(Object thing) {
        if (thing instanceof String) {
            return new Text((String) thing);
        } else if (thing instanceof Long) {
            return new LongWritable((Long) thing);
        } else if (thing instanceof Integer) {
            return new IntWritable((Integer) thing);
        } else if (thing instanceof Double) {
            return new DoubleWritable((Double) thing);
        } else if (thing instanceof Float) {
            return new FloatWritable((Float) thing);
        } else if (thing instanceof Boolean) {
            return new BooleanWritable((Boolean) thing);
        } else if (thing instanceof Map) {
            MapWritable result = new MapWritable();
            for (Map.Entry<String, Object> entry : ((Map<String, Object>) thing).entrySet()) {
                result.put(new Text(entry.getKey().toString()), toWritable(entry.getValue()));
            }
            return result;
        } else if (thing instanceof List) {
            if (((List) thing).size() > 0) {
                Object first = ((List) thing).get(0);
                Writable[] listOfThings = new Writable[((List) thing).size()];
                for (int i = 0; i < listOfThings.length; i++) {
                    listOfThings[i] = toWritable(((List) thing).get(i));
                }
                return new ArrayWritable(toWritable(first).getClass(), listOfThings);
            }
        }
        return NullWritable.get();
    }
}
