package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.OperationContext;
import io.airlift.log.Logger;

import java.util.*;

public class ModelRunnerFlatMapFunction extends SpliceFlatMapFunction<SpliceOperation, Iterator<ExecRow>, ExecRow> implements Iterator<ExecRow> {
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
//    public ModelRunnerFlatMapFunction(){
//        super();
//        this.processedRows = new LinkedList<>();
//
//    }

    @Override
    public boolean hasNext() {
        return !this.processedRows.isEmpty() || this.execRowIterator.hasNext();
    }

    @Override
    public ExecRow next() {
        // If we have any processed rows available, return the first one
        if(this.processedRows.isEmpty()) { // Fill and transform the buffer
            // Fill the buffer until either there are no more rows or we hit our max buffer size
            do {
                this.unprocessedRows.add(this.execRowIterator.next().getClone());
                remainingBufferAvailability--;
            } while (this.execRowIterator.hasNext() && this.remainingBufferAvailability > 0);
            this.remainingBufferAvailability = (this.remainingBufferAvailability <= 0) ? this.maxBufferSize : this.remainingBufferAvailability;

            // transform all rows in buffer
            // return first row
            try {
                switch (this.modelCategory) {
                    case "key_value":
                        this.processedRows = this.runner.predictKeyValue(unprocessedRows, this.modelFeaturesIndexes,
                                this.predictionColIndex, this.predictionLabels, this.predictionLabelIndexes,
                                this.featureColumnNames, this.predictCall, this.predictArgs, this.threshold);
                        break;
                    case "classification":
                        this.processedRows = this.runner.predictClassification(unprocessedRows, this.modelFeaturesIndexes,
                                this.predictionColIndex, this.predictionLabels, this.predictionLabelIndexes,
                                this.featureColumnNames);
                        break;
                    case "regression":
                        this.processedRows = this.runner.predictRegression(unprocessedRows, this.modelFeaturesIndexes,
                                this.predictionColIndex, this.featureColumnNames);
                        break;
                    case "cluster":
                        this.processedRows = this.runner.predictCluster(unprocessedRows, this.modelFeaturesIndexes,
                                this.predictionColIndex, this.featureColumnNames);
                        break;
                }
                this.unprocessedRows.clear();
            } catch (Exception e) {
                e.printStackTrace();
                throw new NoSuchElementException("Could not retrieve next row due to error: " + e.getMessage());
            }
        }
        return this.processedRows.remove();
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
