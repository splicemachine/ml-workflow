package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.catalog.TriggerNewTransitionRows;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import hex.genmodel.easy.exception.PredictException;
import io.airlift.log.Logger;
import jep.JepException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

class CacheClearer extends TimerTask {
    public void run(){
        MLRunner.clearCache();
    }
}

public class MLRunner implements DatasetProvider, VTICosting {

    // For VTI Implementation
    private final String modelCategory, modelID, predictCall, predictArgs, schema, table; //, rawData, schema;
    final List<String> featureColumnNames, predictionLabels;
    final int maxBufferSize;
    private final double threshold;
    TriggerNewTransitionRows newTransitionRows;

    //Provide external context which can be carried with the operation
    protected OperationContext operationContext;
    private static final Logger LOG = Logger.get(MLRunner.class);

    // Model Cache
    protected static final ConcurrentHashMap<String, AbstractRunner> runnerCache = new ConcurrentHashMap<>();

    // Timer to clear the cache every N days
    private static int MILLISECONDS = 86400000; // Milliseconds in a day
    static int CACHE_TIMEOUT_DAYS = 7; // Clear cache after 7 days
    public static Timer timer = new Timer();
    private static CacheClearer cc = new CacheClearer();
    static {timer.scheduleAtFixedRate(cc, 0, CACHE_TIMEOUT_DAYS*MILLISECONDS );}
    public static void clearCache(){
        LOG.info("Timeout reached. Cache cleared");
        runnerCache.clear();
    }

    public static AbstractRunner getRunner(final String modelID)
            throws UnsupportedLibraryExcetion, ClassNotFoundException, SQLException, IOException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        AbstractRunner runner;
        // Check if the runner is in cache
        if (runnerCache.containsKey(modelID)) {
            LOG.info(String.format("Got model %s from cache", modelID));
            runner = runnerCache.get(modelID);
        }
        else {
            // Get the model blob and the library
            final Object[] modelAndLibrary = AbstractRunner.getModelBlob(modelID);
            final String lib = ((String) modelAndLibrary[1]).toLowerCase();
            Blob model = (Blob) modelAndLibrary[0];
            switch (lib) {
                case "h2o":
                    runner = new H2ORunner(model);
                    break;
                case "spark":
                    runner = new MLeapRunner(model);
                    break;
                case "pkl":
                    runner = new SKRunner(model);
                    break;
                case "h5":
                    runner = new KerasRunner(model);
                    break;
                default:
                    // TODO: Review database standards for exceptions
                    throw new UnsupportedLibraryExcetion(
                            "Model library of type " + lib + " is not currently supported for in DB deployment!");
            }
            // Add runner to cache
            runnerCache.put(modelID, runner);
            LOG.info(String.format("Adding model %s to cache", modelID));
        }
        return runner;
    }

    public static boolean checkVals(final String modelID, final String rawData, final String schema){
        return (modelID == null || rawData == null || schema == null);
    }
    public static boolean checkVals(final String str){
        return (str == null);
    }

    /**
     * @deprecated  As of release 2.4.0-k8, VTI only now
     */
    @Deprecated public static String predictClassification(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            UnsupportedLibraryExcetion, ClassNotFoundException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictClassification(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.5.0-k8, VTI only now
     */
    @Deprecated public static Double predictRegression(final String modelID, final String rawData, final String schema)
            throws ClassNotFoundException, UnsupportedLibraryExcetion, SQLException, IOException,
            InvocationTargetException, IllegalAccessException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        //TODO: Add defensive code in case the model returns nothing (ie if a stringindexer skips the row)
        AbstractRunner runner = getRunner(modelID);
        return runner.predictRegression(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.4.0-k8, VTI only now
     */
    @Deprecated public static String predictClusterProbabilities(final String modelID, final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException,
            UnsupportedLibraryExcetion, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictClusterProbabilities(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.4.0-k8, VTI only now
     */
    @Deprecated public static int predictCluster(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            ClassNotFoundException, UnsupportedLibraryExcetion, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return -1;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictCluster(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.4.0-k8, VTI only now
     */
    @Deprecated public static double[] predictKeyValue(final String modelID, final String rawData, final String schema) throws PredictException, ClassNotFoundException, SQLException, UnsupportedLibraryExcetion, IOException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictKeyValue(rawData, schema, null, null, -1);
    }

    public static Double splitResult(final String str, final int index) {
        if (checkVals(str)){
            return null;
        }

        final String v = str.split(";")[index];
        return (Double.valueOf(v.split("=")[1]));
    }

    public static int getPrediction(final String str) {
        if (checkVals(str)){
            return -1;
        }

        final String[] values = str.split(";");
        int maxIndex = 0;
        Double maxVal = 0.0;
        for (int i = 0; i < values.length; i++) {
            final Double num = Double.valueOf(values[i].split("=")[1]);
            if (num > maxVal) {
                maxIndex = i;
                maxVal = num;
            }
        }
        return maxIndex;
    }

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation spliceOperation, DataSetProcessor dataSetProcessor, ExecRow execRow) throws StandardException {

        if (spliceOperation != null)
            operationContext = dataSetProcessor.createOperationContext(spliceOperation);
        else { // this call works even if activation is null
            LOG.warn("Activation is null!");
            operationContext = dataSetProcessor.createOperationContext((Activation) null);
        }

        // Get row indexes of each model/feature column from tabledescriptor so it's always correct even if columns are modified
        List<Integer> modelFeaturesIndexes = new ArrayList<>();
        List<Integer> predictionLabelIndexes = new ArrayList<>();

        ResultColumnDescriptor[] cd = operationContext.getActivation().getLanguageConnectionContext().getTriggerExecutionContext().getTemporaryRowHolder().getResultSet().getResultDescription().getColumnInfo();
        HashMap<String, Integer> colIndexes= new HashMap<>();
        for(ResultColumnDescriptor rcd : cd)    colIndexes.put(rcd.getName(), rcd.getColumnPosition());

        for(String col : this.featureColumnNames) {
            modelFeaturesIndexes.add(colIndexes.get(col.toUpperCase()));
        }
        for(String col : this.predictionLabels){
            if(!col.equalsIgnoreCase("PREDICTION")) { // Keep prediction col separate
                predictionLabelIndexes.add(colIndexes.get(col.toUpperCase()));
            }
        }
        int predictionColIndex;
        try{  predictionColIndex = colIndexes.get("PREDICTION"); }
        catch (Exception e){
            predictionColIndex=-1;
        }


        // Initialize runner
        LOG.warn("Getting runner");
        AbstractRunner runner = null;
        try {
            runner = this.modelCategory.equals("endpoint") ? null : getRunner(this.modelID);
            
        } catch (SQLException e) {
            LOG.error("Unexpected SQLException while getting runner: ", e.getMessage());
        } catch (ClassNotFoundException e) {
            LOG.error("Unexpected ClassNotFoundException while getting runner: ", e.getMessage());
        } catch (Exception e) {
            LOG.error("Unexpected Exception while getting runner: ", e.getMessage());
        }
        LOG.warn("Finished getting runner");
        LOG.warn("Runner is null " + (runner==null));
        assert runner != null: "runner is null!";
        LOG.warn("Got runner " + runner.getTypeFormatId());
        
        // Get the row(s) of data and transform them
        DataSet<ExecRow> rows = this.newTransitionRows.getDataSet(spliceOperation, dataSetProcessor, execRow);
        return rows.mapPartitions(new ModelRunnerFlatMapFunction(operationContext, runner, modelCategory,
                predictCall, predictArgs, threshold, modelFeaturesIndexes, predictionColIndex, predictionLabels,
                predictionLabelIndexes, featureColumnNames, maxBufferSize));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // TODO: We will need to return the a dynamic metadata based on the model in use...
        return null;
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }


    public static DatasetProvider getMLRunner(final String modelCategory, final String modelID, final TriggerNewTransitionRows newTransitionRows,
                                              final String schema, final String table,
                                              final String predictCall, final String predictArgs, final double threshold,
                                              final String featureColumnNames,
                                              final String predictionLabels, final int maxBufferSize) {
        return new MLRunner(modelCategory, modelID, newTransitionRows, schema, table, predictCall, predictArgs,
                threshold, featureColumnNames, predictionLabels, maxBufferSize);
    }
    public static DatasetProvider getMLRunner(){return new MLRunner();}


    // New VTI only
    /**
     * The VTI for MLRunner. This will take the entire New Table reference from the generated STATEMENT trigger
     * and process all of the rows coming in through batches
     * @param modelCategory: the category of the model (regression, classification, cluster, key-value, endpoint)
     * @param modelID: The mlflow run_id associated with the model
     * @param newTransitionRows: The NEW TABLE referenced from the statement trigger
     * @param schema: Schema of the table we are updating
     * @param table: Table we are updating
     * @param predictCall: Optional sklearn call for models to replace predict (predict_proba, transform)
     *                    Will be set to null if not relevant
     * @param predictArgs: Optional sklearn arg for certain models  (return_cov, return std)
     *                    Will be set to null if not relevant
     * @param threshold: Optional keras arg for the threshold of a leaf node for models with 1 leaf node. Will be set
     *                 to null if not relevant
     * @param featureColumnNames: List of column names that are fed into the model
     * @param predictionLabels: The labels of the model predictions
     * @param maxBufferSize: The max size of the buffers for model evaluation. Will default to 10000 from the generated
     *                     trigger
     */
    public MLRunner(final String modelCategory, final String modelID, final TriggerNewTransitionRows newTransitionRows,
                    final String schema, final String table,
                    final String predictCall, final String predictArgs, final double threshold,
                    final String featureColumnNames, final String predictionLabels,
                    final int maxBufferSize){
        this.modelCategory = modelCategory;
        this.modelID = modelID;
        this.newTransitionRows = newTransitionRows;
        this.schema = schema;
        this.table = table;
        this.predictCall = (predictCall != "NULL") ? predictCall  : null;
        this.predictArgs = (predictArgs != "NULL") ? predictArgs  : null;
        this.threshold = threshold;
        this.featureColumnNames = Arrays.asList(featureColumnNames.split(","));
        this.predictionLabels = Arrays.asList(predictionLabels.split(","));
        this.maxBufferSize = maxBufferSize;
    }

    public MLRunner(){
        this.modelCategory = null;
        this.modelID = null;
        this.newTransitionRows = null;
        this.schema = null;
        this.table = null;
        this.predictCall =  null;
        this.predictArgs = null;
        this.threshold = -1;
        this.featureColumnNames = null;
        this.predictionLabels = null;
        this.maxBufferSize = -1;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        //FIXME: We are going to get more than 1 row now
        return 1;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        //FIXME: This should be estimated better
        return 100;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        //FIXME: I think this should be false? We don't want multiple instantiations due to model cache right?
        return true;
    }
}