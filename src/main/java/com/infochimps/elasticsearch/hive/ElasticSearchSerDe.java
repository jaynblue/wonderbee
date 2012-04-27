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
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.*;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.util.JSONPObject;

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
    private static final String ES_IS_JSON = "elasticsearch.is_json";



    @Override
    public void initialize(Configuration conf, Properties properties) throws SerDeException {
        LOG.info("SerDe: "+properties);
        props = properties;
        String columnNameProperty = props.getProperty(Constants.LIST_COLUMNS);
        String columnTypeProperty = props.getProperty(Constants.LIST_COLUMN_TYPES);
        columnNames = Arrays.asList(columnNameProperty.split(","));
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
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
        rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);


    }

    @Override
    public Class<? extends Writable> getSerializedClass() {
        return Text.class;
    }

    @Override
    public Writable serialize(Object obj, ObjectInspector objInspector) throws SerDeException {
        StructObjectInspector outputRowOI = (StructObjectInspector) objInspector;
        List<? extends StructField> outputFieldRefs = outputRowOI
                .getAllStructFieldRefs();
        MapWritable record = new MapWritable();

        String isJson = props.getProperty(ES_IS_JSON);
        if ("true".equalsIgnoreCase(isJson)) {
            throw new SerDeException("Json mode not yet supported");
        }
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
        String jsonText = ((Text) writable).toString();
        JsonNode json;
        try {
            json = mapper.readTree(jsonText);
        } catch (IOException e) {
            throw new SerDeException(e);

        }
        List<Object> result = new ArrayList<Object>();
        for (int i = 0; i < numColumns; i++) {
            // LOG.error("Processing column: " + i + " name: " + columnNames.get(i));
            String columnName = columnNames.get(i);
            JsonNode jsonValue = json.get(columnName);
            Object value = null;
            if (jsonValue != null) {
                TypeInfo type = columnTypes.get(i);
                if (type.getTypeName().equals(Constants.BIGINT_TYPE_NAME)) {
                    value = jsonValue.getLongValue();
                } else if (type.getTypeName().equals(Constants.STRING_TYPE_NAME)) {
                    value = jsonValue.getTextValue();
                } else if (type.getTypeName().equals(Constants.INT_TYPE_NAME)) {
                    value = jsonValue.getIntValue();
                } else if (type.getTypeName().equals(Constants.FLOAT_TYPE_NAME)) {
                    value = jsonValue.getNumberValue().floatValue();
                } else if (type.getTypeName().equals(Constants.BOOLEAN_TYPE_NAME)) {
                    value = jsonValue.getBooleanValue();
                } else if (type.getTypeName().equals(Constants.DOUBLE_TYPE_NAME)) {
                    value = jsonValue.getDoubleValue();
                }
            }

            result.add(value);
        }
        return result;
    }

    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return rowOI;
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
