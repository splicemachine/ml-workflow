package com.splicemachine.mlrunner2;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

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
    AbstractRunner runner;

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
            throws ClassNotFoundException, SQLException, IOException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        AbstractRunner runner;
        // Check if the runner is in cache
        if (runnerCache.containsKey(modelID)) {
            LOG.info(String.format("Run_ID: %1$s - Got model %1$s from cache", modelID));
            runner = runnerCache.get(modelID);
        }
        else {
            // Get the model blob and the library
            Object[] modelAndLibrary;
            synchronized(AbstractRunner.class) {
                if (runnerCache.containsKey(modelID)) {
                    LOG.info(String.format("Run_ID: %1$s - Got model %1$s from cache in another thread", modelID));
                    runner = runnerCache.get(modelID);
                    return runner;

                }

                modelAndLibrary = AbstractRunner.getModelBlob(modelID);
                final String lib = ((String) modelAndLibrary[1]).toLowerCase();
                byte[] model = (byte[]) modelAndLibrary[0];
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
                        String err = "Run_ID: " + modelID + "Model library of type " + lib + " is not currently " +
                                "supported for Database model deployment!";
                        LOG.error(err);
                        StandardException se = StandardException.newException(LANG_INTERNAL_ERROR, err);
                        throw new RuntimeException(se);
                }
                // Add runner to cache
                runnerCache.put(modelID, runner);
                LOG.info(String.format("Run_ID: %1$s - Adding %2$s model %1$s to cache", modelID, lib));
            }
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
     * @deprecated  As of release 2.6.0-k8, VTI only now
     */
    @Deprecated public static String predictClassification(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            UnsupportedLibraryException, ClassNotFoundException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException, StandardException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictClassification(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.6.0-k8, VTI only now
     */
    @Deprecated public static Double predictRegression(final String modelID, final String rawData, final String schema)
            throws ClassNotFoundException, UnsupportedLibraryException, SQLException, IOException,
            InvocationTargetException, IllegalAccessException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException, StandardException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        //TODO: Add defensive code in case the model returns nothing (ie if a stringindexer skips the row)
        AbstractRunner runner = getRunner(modelID);
        return runner.predictRegression(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.6.0-k8, VTI only now
     */
    @Deprecated public static String predictClusterProbabilities(final String modelID, final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException,
            UnsupportedLibraryException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException, StandardException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return null;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictClusterProbabilities(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.6.0-k8, VTI only now
     */
    @Deprecated public static int predictCluster(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            ClassNotFoundException, UnsupportedLibraryException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException, StandardException {
        // Defensive in case no values are passed into the function
        if (checkVals(modelID, rawData, schema)){
            return -1;
        }

        AbstractRunner runner = getRunner(modelID);
        return runner.predictCluster(rawData, schema);
    }

    /**
     * @deprecated  As of release 2.6.0-k8, VTI only now
     */
    @Deprecated public static double[] predictKeyValue(final String modelID, final String rawData, final String schema) throws PredictException, ClassNotFoundException, SQLException, UnsupportedLibraryException, IOException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException, StandardException {
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

        LanguageConnectionContext languageConnectionContext = operationContext.getActivation().getLanguageConnectionContext();

        ResultColumnDescriptor[] cd = languageConnectionContext.getTriggerExecutionContext().getTemporaryRowHolder().getResultSet().getResultDescription().getColumnInfo();
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
        LOG.info("RUN_ID: " + this.modelID + " - Getting runner");
        try {
            this.runner = this.modelCategory.equals("endpoint") ? null : getRunner(this.modelID);
        } catch (SQLException e) {
            // A StandardException of LANG_INTERNAL_ERROR will get propagated to the user so they can know the problem
            String err = "Run_ID: " + modelID + " - Unexpected SQLException while getting runner: " +
                    "The following exception was thrown: " + e.getMessage();
            LOG.error(err);
            StandardException se = StandardException.newException(LANG_INTERNAL_ERROR, err);
            throw new RuntimeException(se);
        } catch (ClassNotFoundException e) {
            String err = "Run_ID: " + modelID + " - Unexpected ClassNotFoundException while getting runner: " +
                    "The following exception was thrown: " + e.getMessage();
            LOG.error(err);
            StandardException se = StandardException.newException(LANG_INTERNAL_ERROR, err);
            throw new RuntimeException(se);
        } catch (Exception e) {
            String err = "Run_ID: " + modelID + " - Unexpected Exception while getting runner: " +
                    "The following exception was thrown: " + e.getMessage();
            LOG.error(err);
            StandardException se = StandardException.newException(LANG_INTERNAL_ERROR, err);
            throw new RuntimeException(se);
        }
        LOG.warn("RUN_ID: " + this.modelID + " - Finished getting runner");
        LOG.warn("Runner is null " + (runner==null));
        assert runner != null: "runner is null!";

        // Get the row(s) of data and transform them
        DataSet<ExecRow> rows = this.newTransitionRows.getDataSet(spliceOperation, dataSetProcessor, execRow);
        return rows.mapPartitions(new ModelRunnerFlatMapFunction(operationContext, runner, modelCategory,
                predictCall, predictArgs, threshold, modelFeaturesIndexes, predictionColIndex, predictionLabels,
                predictionLabelIndexes, featureColumnNames, maxBufferSize, modelID));
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
        this.predictCall = (!predictCall.equals("NULL")) ? predictCall  : null;
        this.predictArgs = (!predictArgs.equals("NULL")) ? predictArgs  : null;
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
        return 100000;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        //FIXME: This should be estimated better
        return 100000;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        //FIXME: I think this should be false? We don't want multiple instantiations due to model cache right?
        return true;
    }
}