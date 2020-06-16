package com.splicemachine.mlrunner;

import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.core.types.DataType;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder;
import ml.combust.mleap.tensor.Tensor;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.sql.*;
import java.util.Collections;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.regex.Pattern;

import hex.genmodel.easy.exception.PredictException;

/**
 * Scalar function for making predictions via mleap
 */
public class MLeapRunner extends AbstractRunner {
    private static LeapFrameBuilder frameBuilder = new LeapFrameBuilder();
    Transformer model;
    static Class[] parameterTypes = { String.class };

    public MLeapRunner(final Blob modelBlob) throws IOException, ClassNotFoundException, SQLException {
        final InputStream bis = modelBlob.getBinaryStream();
        final ObjectInputStream ois = new ObjectInputStream(bis);
        this.model = (Transformer) ois.readObject();
    }

    private static HashMap<String, DataType> typeConversions = new HashMap<String, DataType>() {
        {
            put("FLOAT", frameBuilder.createFloat());
            put("BIGINT", frameBuilder.createLong());
            put("INT", frameBuilder.createInt());
            put("INTEGER", frameBuilder.createInt());
            put("SMALLINT", frameBuilder.createShort());
            put("TINYINT", frameBuilder.createByte());
            put("BOOLEAN", frameBuilder.createBoolean());
            put("CHAR", frameBuilder.createString());
            put("VARCHAR", frameBuilder.createString());
            put("DATE", frameBuilder.createString());
            put("TIME", frameBuilder.createString());
            put("TIMESTAMP", frameBuilder.createString());
            put("DECIMAL", frameBuilder.createDouble());
            put("NUMERIC", frameBuilder.createDouble());
            put("DOUBLE", frameBuilder.createDouble());
            put("REAL", frameBuilder.createDouble());
        }

    };
    private static HashMap<String, Method> converters = new HashMap<String, Method>() {
        {
            try {
                put("DOUBLE", MLeapRunner.class.getMethod("toDouble", parameterTypes));
                put("FLOAT", MLeapRunner.class.getMethod("toFloat", parameterTypes));
                put("BIGINT", MLeapRunner.class.getMethod("toLong", parameterTypes));
                put("INT", MLeapRunner.class.getMethod("toInt", parameterTypes));
                put("INTEGER", MLeapRunner.class.getMethod("toInt", parameterTypes));
                put("SMALLINT", MLeapRunner.class.getMethod("toShort", parameterTypes));
                put("TINYINT", MLeapRunner.class.getMethod("toByte", parameterTypes));
                put("BOOLEAN", MLeapRunner.class.getMethod("toBool", parameterTypes));
                put("CHAR", MLeapRunner.class.getMethod("toStr", parameterTypes));
                put("VARCHAR", MLeapRunner.class.getMethod("toStr", parameterTypes));
                put("DATE", MLeapRunner.class.getMethod("toStr", parameterTypes));
                put("TIME", MLeapRunner.class.getMethod("toStr", parameterTypes));
                put("TIMESTAMP", MLeapRunner.class.getMethod("toStr", parameterTypes));
                put("DECIMAL", MLeapRunner.class.getMethod("toDouble", parameterTypes));
                put("NUMERIC", MLeapRunner.class.getMethod("toDouble", parameterTypes));
                put("REAL", MLeapRunner.class.getMethod("toDouble", parameterTypes));
            } catch (final NoSuchMethodException e) {
                e.printStackTrace();
            }
        }
    };

    public static Double toDouble(final String d) {
        return Double.valueOf(d);
    }

    public static Float toFloat(final String d) {
        return Float.valueOf(d);
    }

    public static Long toLong(final String d) {
        return Long.valueOf(d);
    }

    public static Integer toInt(final String d) {
        return Integer.valueOf(d);
    }

    public static Short toShort(final String d) {
        return Short.valueOf(d);
    }

    public static Byte toByte(final String d) {
        return Byte.valueOf(d);
    }

    public static Boolean toBool(final String d) {
        return Boolean.valueOf(d);
    }

    public static String toStr(final String d) {
        return d;
    }

    private static <E> Seq<E> createScalaSequence(final ArrayList<E> javaList) {
        return JavaConverters.collectionAsScalaIterableConverter(javaList).asScala().toSeq();
    }

    /**
     * Function to create a LeapFrame given the features in String format and the
     * schema of the dataset in String format Function will properly convert values
     * to String/Float/Double etc based on the provided schema
     *
     * @param rawData: A comma separated list of strings
     * @param schema:  A String of SQL Column List format containing comma separated
     *                 column name and datatype. Ex 'A INTEGER, B FLOAT'
     * @return DefaultLeapFrame reconstructed from the strings
     */

    private DefaultLeapFrame parseDataToFrame(final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException {
        // Schema parsing setup
        final Pattern p = Pattern.compile("[^A-Za-z]");
        final String[] schemaStrings = schema.split(",");
        final StructField[] structFields = new StructField[schemaStrings.length];
        // Raw data parse setup
        final String[] splits = rawData.split(",");
        final Object[] features = new Object[splits.length];

        // Parsing data and creating LeapFrame
        for (int i = 0; i < schemaStrings.length; i++) {
            // get the name and type of the column
            final String schStr = schemaStrings[i].trim();
            final String col = schStr.trim().split(" ")[0];
            String dataType = schStr.substring(col.length());
            dataType = p.matcher(dataType).replaceAll("");
            // Create the StructField and properly convert the raw value
            structFields[i] = frameBuilder.createField(col, typeConversions.get(dataType));
            features[i] = converters.get(dataType).invoke(null, splits[i]);
        }
        // Create our LeapFrame
        final StructType schemaStruct = frameBuilder.createSchema(Arrays.asList(structFields));
        final Row featureSet = frameBuilder.createRowFromIterable(Arrays.asList(features));
        final DefaultLeapFrame frame = frameBuilder.createFrame(schemaStruct, Arrays.asList(featureSet));
        return frame;
    }

    @Override
    public String predictClassification(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Define out desired output column(s)
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("probability"));
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();
        final Tensor probs = output.select(createScalaSequence(outputCols)).get().dataset().iterator().next()
                .getTensor(0);
        final Iterator tensorIterator = probs.rawValuesIterator();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < probs.size(); i++) {
            builder.append(i).append("=").append(tensorIterator.next().toString()).append(";");
        }
        final String res = builder.toString();
        return res.substring(0, res.length() - 1);
    }

    @Override
    public Double predictRegression(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Define out desired output column(s)
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();
        final Double pred = output.select(createScalaSequence(outputCols)).get().collect().last().getDouble(0);
        return pred;
    }

    @Override
    public String predictClusterProbabilities(final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            ClassNotFoundException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();

        // Define out desired output columns
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("probability"));
        final Tensor probs = output.select(MLeapRunner.createScalaSequence(outputCols)).get().dataset().iterator().next()
                .getTensor(0);
        final Iterator tensorIterator = probs.rawValuesIterator();
        final StringBuilder builder = new StringBuilder();
        for (int i = 0; i < probs.size(); i++) {
            builder.append(i).append("=").append(tensorIterator.next().toString()).append(";");
        }
        final String res = builder.toString();
        return res.substring(0, res.length() - 1);
    }

    public int predictCluster(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        // Get the prediction
        final int pred = output.select(MLeapRunner.createScalaSequence(outputCols)).get().collect().last().getInt(0);
        return pred;
    }

    @Override
    public double[] predictKeyValue(final String rawData, final String schema, final String predictCall, final String predictArgs, double threshold) throws PredictException {
        return null;
    }
}


