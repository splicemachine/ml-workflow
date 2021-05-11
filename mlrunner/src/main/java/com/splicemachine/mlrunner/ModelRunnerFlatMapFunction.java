package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.ArrayImpl;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;

import io.airlift.log.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLException;
import java.util.*;

public class ModelRunnerFlatMapFunction extends SpliceFlatMapFunction<SpliceOperation, Iterator<ExecRow>, ExecRow> implements Iterator<ExecRow>, Externalizable {
    private static final Logger LOG = Logger.get(MLRunner.class);
    AbstractRunner runner;
    String modelCategory, predictCall, predictArgs;
    List<Integer> modelFeaturesIndexes, predictionLabelIndexes;
    List<String> predictionLabels, featureColumnNames;
    double threshold;
    int maxBufferSize, remainingBufferAvailability, predictionColIndex;

    LinkedList<ExecRow> unprocessedRows;
    Queue<ExecRow> processedRows;
    Iterator<ExecRow> execRowIterator;

    /**
     * The override mapPartitions for the MLRunner VTI
     * @param operationContext
     * @param runner: The model runner of the serialized ML model
     * @param modelCategory: the category of the model (regression, classification, cluster, cluster_prob, key-value)
     * @param predictCall: Optional sklearn call for models to replace predict (predict_proba, transform)
     *                   Will be set to null if not relevant
     * @param predictArgs: Optional sklearn arg for certain models  (return_cov, return std)
     *                    Will be set to null if not relevant
     * @param threshold: Optional keras arg for the threshold of a leaf node for models with 1 leaf node. Will be set
     *                 to -1 if not relevant
     * @param modelFeaturesIndexes: The list of indexes in the row of the columns that are used by the model. Indexes for
     *                            rows start at 1.
     * @param predictionColIndex: The column in the row of the prediction column.
     * @param predictionLabels: The labels of the model predictions
     * @param maxBufferSize: The max size of the buffers for model evaluation. Will default to 10000 from the generated
     *                     trigger
     */
    public ModelRunnerFlatMapFunction(
            OperationContext operationContext, final AbstractRunner runner, final String modelCategory, final String predictCall,
            final String predictArgs, final double threshold, final List<Integer> modelFeaturesIndexes,
            final int predictionColIndex, final List<String> predictionLabels, final List<Integer> predictionLabelIndexes,
            final List<String> featureColumnNames, final int maxBufferSize){

        super(operationContext);
        LOG.warn("Proper constructor called");
        this.runner = runner;
        assert runner != null: "Runner is null!";
        this.modelCategory = modelCategory;
        this.predictCall = predictCall;
        this.predictArgs = predictArgs;
        this.threshold = threshold;
        this.modelFeaturesIndexes = modelFeaturesIndexes;
        this.predictionColIndex = predictionColIndex;
        this.predictionLabels = predictionLabels;
        this.predictionLabelIndexes = predictionLabelIndexes;
        this.featureColumnNames = featureColumnNames;
        this.maxBufferSize = maxBufferSize;

        this.remainingBufferAvailability = this.maxBufferSize;
        this.unprocessedRows = new LinkedList<>();
        this.processedRows = new LinkedList<>();
    }
    public ModelRunnerFlatMapFunction(){
        super();
        LOG.warn("Default constructor called");
        this.unprocessedRows = new LinkedList<>();
        this.processedRows = new LinkedList<>();
    }

    @Override
    public boolean hasNext() {
        return !this.processedRows.isEmpty() || this.execRowIterator.hasNext();
    }

    @Override
    public ExecRow next() throws SQLException{
        // If we have any processed rows available, return the first one
        if(this.processedRows.isEmpty()) { // Fill and transform the buffer
            // Fill the buffer until either there are no more rows or we hit our max buffer size
            do {
                this.unprocessedRows.add(this.execRowIterator.next().getClone());
                this.remainingBufferAvailability--;
            } while (this.execRowIterator.hasNext() && this.remainingBufferAvailability > 0);
            this.remainingBufferAvailability = (this.remainingBufferAvailability <= 0) ? this.maxBufferSize : this.remainingBufferAvailability;

            // transform all rows in buffer
            // return first row
            try {
                LOG.info("Model Category: " + modelCategory + " doing transformation of size " + unprocessedRows.size());
                switch (modelCategory) {
                    case "key_value":
                        this.processedRows = this.runner.predictKeyValue(unprocessedRows, modelFeaturesIndexes,
                                predictionColIndex, predictionLabels, predictionLabelIndexes,
                                featureColumnNames, predictCall, predictArgs, threshold);
                        break;
                    case "classification":
                        this.processedRows = this.runner.predictClassification(unprocessedRows, modelFeaturesIndexes,
                                predictionColIndex, predictionLabels, predictionLabelIndexes, featureColumnNames);
                        break;
                    case "regression":
                        this.processedRows = this.runner.predictRegression(unprocessedRows, modelFeaturesIndexes,
                                predictionColIndex, featureColumnNames);
                        break;
                    case "cluster":
                        this.processedRows = this.runner.predictCluster(unprocessedRows, modelFeaturesIndexes,
                                predictionColIndex, featureColumnNames);
                        break;
                }
                this.unprocessedRows.clear();
            } catch (Exception e) {
                e.printStackTrace();
                throw new NoSuchElementException("Could not retrieve next row due to error: " + e.getMessage());
            }
        }
        return processedRows.remove();
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> execRowIterator) throws Exception {
        this.execRowIterator = execRowIterator;
        return this;
    }
    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.runner); //AbstractRunner
        out.writeUTF(this.modelCategory);
        out.writeUTF((this.predictCall != null) ? this.predictCall  : "NULL"); // Cannot write a null, get NPE
        out.writeUTF((this.predictArgs != null) ? this.predictArgs  : "NULL"); // Cannot write a null, get NPE
        out.writeDouble(this.threshold);
        out.writeInt(this.predictionColIndex);
        out.writeInt(this.maxBufferSize);
        // Need to use ArrayImpl because it implements readExternal and writeExternal
        // https://github.com/splicemachine/spliceengine/blob/master/db-shared/src/main/java/com/splicemachine/db/iapi/types/ArrayImpl.java
        out.writeObject(new ArrayImpl("null",-1,this.modelFeaturesIndexes.toArray())); //List<Integer>
        out.writeObject(new ArrayImpl("null",-1,this.predictionLabels.toArray())); //List<String>
        out.writeObject(new ArrayImpl("null",-1,this.predictionLabelIndexes.toArray())); //List<Integer>
        out.writeObject(new ArrayImpl("null",-1,this.featureColumnNames.toArray())); //List<String>
    }

    public List<Integer> arrayImplToIntList(ArrayImpl array) throws SQLException {
        Object[] mfio = (Object[]) array.getArray();
        Integer[] mfii = Arrays.asList(mfio).toArray(new Integer[0]);
        return Arrays.asList(mfii);
    }
    public List<String> arrayImplToStringList(ArrayImpl array) throws SQLException {
        Object[] mfio = (Object[]) array.getArray();
        String[] mfii = Arrays.asList(mfio).toArray(new String[0]);
        return Arrays.asList(mfii);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.runner = (AbstractRunner) in.readObject();
        this.modelCategory = in.readUTF();
        this.predictCall = in.readUTF();
        this.predictCall = (predictCall.equals("NULL")) ? null : predictCall; // Convert back in case it was null
        this.predictArgs = in.readUTF();
        this.predictArgs = (predictArgs.equals("NULL")) ? null : predictArgs; // Convert back in case it was null
        this.threshold = in.readDouble();
        this.predictionColIndex = in.readInt();
        this.maxBufferSize = in.readInt();

        try {
            // Need to convert arrayimpl back into an array, then to List
            // convert in.readObject() to an ArrayImpl class. Then get the array. Then convert that Object to a Integer[]
            // Then to a List<Integer>
//            this.modelFeaturesIndexes = Arrays.asList(((Integer[]) ((ArrayImpl) in.readObject()).getArray()));

//            ArrayImpl mfia = (ArrayImpl) in.readObject();
//            Object[] mfio = (Object[]) mfia.getArray();
//            Integer[] mfii = Arrays.asList(mfio).toArray(new Integer[0]);
//            this.modelFeaturesIndexes = Arrays.asList(mfii);

            this.modelFeaturesIndexes = arrayImplToIntList((ArrayImpl) in.readObject());

//            ArrayImpl pla = (ArrayImpl) in.readObject();
//            Object[] plo = (Object[]) pla.getArray();
//            String[] pls = Arrays.asList(plo).toArray(new String[0]);
//            this.predictionLabels = Arrays.asList(pls);
            this.predictionLabels = arrayImplToStringList((ArrayImpl) in.readObject());


//            ArrayImpl plia = (ArrayImpl) in.readObject();
//            Object[] plio = (Object[]) plia.getArray();
//            Integer[] plii = Arrays.asList(plio).toArray(new Integer[0]);
//            this.predictionLabelIndexes = Arrays.asList(plii);

            this.predictionLabelIndexes = arrayImplToIntList((ArrayImpl) in.readObject());

            this.featureColumnNames = arrayImplToStringList((ArrayImpl) in.readObject());

//            ArrayImpl fcna = (ArrayImpl) in.readObject();
//            Object[] fcno = (Object[]) fcna.getArray();
//            String[] fcns = Arrays.asList(fcno).toArray(new String[0]);
//            this.featureColumnNames = Arrays.asList(fcns);

//            this.predictionLabels = Arrays.asList(((String[]) ((ArrayImpl) in.readObject()).getArray()));
//            this.predictionLabelIndexes = Arrays.asList(((Integer[]) ((ArrayImpl) in.readObject()).getArray()));
//            this.featureColumnNames = Arrays.asList(((String[]) ((ArrayImpl) in.readObject()).getArray()));
        }
        catch(SQLException sqlException){
            throw new IOException("Could not deserialize arraylists due to error: " + sqlException.getMessage());
        }
    }
}

