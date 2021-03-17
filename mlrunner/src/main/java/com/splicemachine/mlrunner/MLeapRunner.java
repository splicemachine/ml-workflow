package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLVarchar;
import io.airlift.log.Logger;
import ml.combust.mleap.core.types.DataType;
import ml.combust.mleap.core.types.StructField;
import ml.combust.mleap.core.types.StructType;
import ml.combust.mleap.runtime.frame.DefaultLeapFrame;
import ml.combust.mleap.runtime.frame.Row;
import ml.combust.mleap.runtime.frame.Transformer;
import ml.combust.mleap.runtime.javadsl.LeapFrameBuilder;
import ml.combust.mleap.tensor.Tensor;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Scalar function for making predictions via mleap
 */
public class MLeapRunner extends AbstractRunner implements Formatable {
    private static LeapFrameBuilder frameBuilder = new LeapFrameBuilder();
    // For serializing and deserializing across spark
    SQLBlob deserModel;

    Transformer model;

    static Class[] parameterTypes = { String.class };
    private static final Logger LOG = Logger.get(MLRunner.class);

    public MLeapRunner(){};
    public MLeapRunner(final Blob modelBlob) throws IOException, ClassNotFoundException, SQLException {
        this.deserModel = new SQLBlob(modelBlob);
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


    private DefaultLeapFrame parseDataToFrame(LinkedList<ExecRow> unprocessedRows, List<Integer> modelFeaturesIndexes,
                                              List<String> featureColumnNames)
            throws StandardException, InvocationTargetException, IllegalAccessException {

        List <Row> frameRows = new ArrayList<>();
        int numFeatures = modelFeaturesIndexes.size();
        Object [] rowData = new Object[numFeatures]; // The rows' datum
        final StructField[] structFields = new StructField[numFeatures];
        StructType schemaStruct = null; // The row's schema

        java.util.Iterator<ExecRow> unpr = unprocessedRows.descendingIterator();
        while(unpr.hasNext()){
            ExecRow row = unpr.next();
            for(int ind = 0; ind < numFeatures; ind ++){
                DataValueDescriptor column = row.getColumn(modelFeaturesIndexes.get(ind));
                String dataType = column.getTypeName();
                // Create the structField for the given column of the row
                // Only 1 StructType per LeapFrame
                if(schemaStruct == null)    structFields[ind] = frameBuilder.createField(featureColumnNames.get(ind), typeConversions.get(dataType));
                rowData[ind] = converters.get(dataType).invoke(null, column.getString());
            }
            if(schemaStruct == null)    schemaStruct = frameBuilder.createSchema(Arrays.asList(structFields));
            frameRows.add(frameBuilder.createRowFromIterable(Arrays.asList(rowData)));
        }
        return frameBuilder.createFrame(schemaStruct, frameRows);
    }

    /**
     * @deprecated as of 2.4.0-k8, replaced by {@link #parseDataToFrame(LinkedList, List, List)}
     * Function to create a LeapFrame given the features in String format and the
     * schema of the dataset in String format Function will properly convert values
     * to String/Float/Double etc based on the provided schema
     *
     * @param rawData: A comma separated list of strings
     * @param schema:  A String of SQL Column List format containing comma separated
     *                 column name and datatype. Ex 'A INTEGER, B FLOAT'
     * @return DefaultLeapFrame reconstructed from the strings
     */

    @Deprecated private DefaultLeapFrame parseDataToFrame(final String rawData, final String schema)
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
    public Queue<ExecRow> predictClassification(final LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes,
                                                int predictionColIndex, List<String> predictionLabels,
                                                List<Integer> predictionLabelIndexes, List<String> featureColumnNames)
            throws IllegalAccessException, StandardException, InvocationTargetException {

        LOG.warn("Classification called");
        Queue<ExecRow> transformedRows = new LinkedList<>();
        final DefaultLeapFrame frame = parseDataToFrame(rows, modelFeaturesIndexes, featureColumnNames);
        final ArrayList<String> outputCols = new ArrayList<>(Collections.singletonList("probability"));
        final DefaultLeapFrame output = this.model.transform(frame).get();
        LOG.warn("A prediction has been made on " + rows.size() + " rows!");
        Iterator<Row> predictedRows = output.select(createScalaSequence(outputCols)).get().dataset().iterator();
        while(predictedRows.hasNext()){ // Loop through the predicted rows
            final Tensor probs = predictedRows.next().getTensor(0);
            final Iterator tensorIterator = probs.rawValuesIterator();
            ExecRow nextRow = rows.remove();

            int predCol = 0;
            Double maxValue = 0.0; // the max probability (for prediction set)
            int maxIndex = -1;   // index of max probability
            while(tensorIterator.hasNext()){ // Loop through the probabilities of the given row
                // Alter the original row and set the probabilities
                Double prob = (Double) tensorIterator.next();
                nextRow.setColumn(predictionLabelIndexes.get(predCol), new SQLDouble(prob));
                // Compare to current max
                if(prob > maxValue){
                    maxValue = prob;
                    maxIndex = predCol;
                }
                predCol ++;

            }
            nextRow.setColumn(predictionColIndex, new SQLVarchar(predictionLabels.get(maxIndex)));
            transformedRows.add(nextRow);
        }
        LOG.warn("Done transforming " + transformedRows.size() + " rows");
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictRegression(final LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes,
                                            int predictionColIndex, List<String> featureColumnNames)
            throws IllegalAccessException, StandardException, InvocationTargetException {

        Queue<ExecRow> transformedRows = new LinkedList<>();
        final DefaultLeapFrame frame = parseDataToFrame(rows, modelFeaturesIndexes, featureColumnNames);
        // Define out desired output column(s)
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        final DefaultLeapFrame output = this.model.transform(frame).get();
        Iterator<Row> predictedRows = output.select(createScalaSequence(outputCols)).get().dataset().iterator();
        while(predictedRows.hasNext()) { // Loop through the predicted rows
            double pred = predictedRows.next().getDouble(0);
            ExecRow row = rows.remove();
            row.setColumn(predictionColIndex, new SQLDouble(pred));
            transformedRows.add(row);
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictClusterProbabilities(final LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes,
                                                      int predictionColIndex, List<String> predictionLabels,
                                                      List<Integer> predictionLabelIndexes, List<String> featureColumnNames)
            throws IllegalAccessException, StandardException, InvocationTargetException {

        return predictClassification(rows, modelFeaturesIndexes, predictionColIndex, predictionLabels,
                predictionLabelIndexes, featureColumnNames);
    }

    @Override
    public Queue<ExecRow> predictCluster(final LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes,
                                         int predictionColIndex, List<String> featureColumnNames)
            throws IllegalAccessException, StandardException, InvocationTargetException {

        Queue<ExecRow> transformedRows = new LinkedList<>();
        final DefaultLeapFrame frame = parseDataToFrame(rows, modelFeaturesIndexes, featureColumnNames);
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        final DefaultLeapFrame output = this.model.transform(frame).get();
        Iterator<Row> predictedRows = output.select(createScalaSequence(outputCols)).get().dataset().iterator();
        while(predictedRows.hasNext()) { // Loop through the predicted rows
            int pred = ((Number)predictedRows.next().get(0)).intValue();
            ExecRow row = rows.remove();
            row.setColumn(predictionColIndex, new SQLDouble(pred));
            transformedRows.add(row);
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictKeyValue(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, String predictCall, String predictArgs, double threshold) {
        return null;
    }

    @Override
    @Deprecated public String predictClassification(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException {
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
    @Deprecated public Double predictRegression(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Define out desired output column(s)
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();
        final Double pred = output.select(createScalaSequence(outputCols)).get().collect().last().getDouble(0);
        return pred;
    }

    @Override
    @Deprecated public String predictClusterProbabilities(final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException{
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

    @Deprecated public int predictCluster(final String rawData, final String schema) throws InvocationTargetException,
            IllegalAccessException {
        final DefaultLeapFrame frame = parseDataToFrame(rawData, schema);
        // Run the model
        final DefaultLeapFrame output = this.model.transform(frame).get();
        final ArrayList<String> outputCols = new ArrayList<String>(Collections.singletonList("prediction"));
        // Get the prediction (sometimes a double like 0.0 is returned so we can't call getInt() directly)
        final int pred = ((Number)output.select(MLeapRunner.createScalaSequence(outputCols)).get().collect().last().get(0)).intValue();
        return pred;
    }

    @Override
    public double[] predictKeyValue(final String rawData, final String schema, final String predictCall, final String predictArgs, double threshold)  {
        return null;
    }

    //////////////////////////////////////////////
    //
    // FORMATABLE
    // Mleap model class Transformer does not implement Serializable so we need to implement it on the SQLBlob (which does)
    //
    //////////////////////////////////////////////

    /** @exception  IOException thrown on error */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(deserModel);
    }

    /**
     * @see java.io.Externalizable#readExternal
     *
     * @exception IOException on error
     * @exception ClassNotFoundException on error
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SQLBlob sqlModelBlob = (SQLBlob) in.readObject();
        Blob modelBlob;
        InputStream bis = null;
        try {
            modelBlob = (Blob) (sqlModelBlob.getObject());
            bis = modelBlob.getBinaryStream();
        } catch (StandardException | SQLException e) {
            e.printStackTrace();
        }
        final ObjectInputStream ois = new ObjectInputStream(bis);
        this.model = (Transformer) ois.readObject();
    }

    @Override
    public int getTypeFormatId() {return super.getTypeFormatId();}
}


