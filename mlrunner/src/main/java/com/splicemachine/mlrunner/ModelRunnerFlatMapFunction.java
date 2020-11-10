package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.util.*;

public class ModelRunnerFlatMapFunction extends SpliceFlatMapFunction<SpliceOperation, Iterator<ExecRow>, ExecRow> implements Iterator<ExecRow> {

    final AbstractRunner runner;
    final String modelCategory, predictCall, predictArgs;
    final List<Integer> modelFeaturesIndexes, predictionLabelIndexes;
    final List<String> predictionLabels, featureColumnNames;
    final double threshold;
    int maxBufferSize, remainingBufferAvailability, predictionColIndex;

    Queue<ExecRow> unprocessedRows;
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
        this.runner = runner;
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

    @Override
    public boolean hasNext() {
        return !this.processedRows.isEmpty() || this.execRowIterator.hasNext();
    }

    @Override
    public ExecRow next(){

        // If we have any processed rows available, return the first one
        if(this.processedRows.isEmpty()) {

            // Fill the buffer until either there are no more rows or we hit our max buffer size
            while (this.execRowIterator.hasNext() && this.remainingBufferAvailability != 0) {
                this.unprocessedRows.add(this.execRowIterator.next());
                remainingBufferAvailability--;
            }
        }
        // transform all rows in buffer
        // return first row
        //TODO: this.processedRows = this.runner.predictClassification(this.unprocessedRows)
        this.unprocessedRows.clear();
        return this.processedRows.remove();

//        batchRows.add(this.execRowIterator.next().getClone());
//        ExecRow input = this.execRowIterator.next().getClone();
//        SQLInteger newVal = new SQLInteger(input.getInt(1)+ 1) ;
//        input.setColumn(1, newVal);
//        return input;
    }

    @Override
    public void remove() {
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> execRowIterator) throws Exception {
        this.execRowIterator = execRowIterator;
        return this;
    }
}
