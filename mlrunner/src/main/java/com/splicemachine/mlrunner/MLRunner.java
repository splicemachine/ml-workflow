package com.splicemachine.mlrunner;
import java.sql.*;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import hex.genmodel.easy.exception.PredictException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import io.airlift.log.Logger;
import jep.JepException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.InvalidKerasConfigurationException;
import org.deeplearning4j.nn.modelimport.keras.exceptions.UnsupportedKerasConfigurationException;


public class MLRunner implements DatasetProvider, VTICosting {

    // For VTI Implementation
    private final String modelCategory,  modelID, rawData, schema;
    private final String predictCall;
    private final String predictArgs;
    private final double threshold;
    //Provide external context which can be carried with the operation
    protected OperationContext operationContext;
    private static final Logger LOG = Logger.get(MLRunner.class);

    public static AbstractRunner getRunner(final String modelID)
            throws UnsupportedLibraryExcetion, ClassNotFoundException, SQLException, IOException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        // Get the model blob and the library
        final Object[] modelAndLibrary = AbstractRunner.getModelBlob(modelID);
        final String lib = (String) modelAndLibrary[1];
        AbstractRunner runner;
        Blob model = (Blob) modelAndLibrary[0];
        switch (lib.toLowerCase()) {
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
        return runner;
    }

    public static String predictClassification(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            UnsupportedLibraryExcetion, ClassNotFoundException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {

            AbstractRunner runner = getRunner(modelID);
            return runner.predictClassification(rawData, schema);
    }

    public static Double predictRegression(final String modelID, final String rawData, final String schema)
            throws ClassNotFoundException, UnsupportedLibraryExcetion, SQLException, IOException,
            InvocationTargetException, IllegalAccessException, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        //TODO: Add defensive code in case the model returns nothing (ie if a stringindexer skips the row)
        AbstractRunner runner = getRunner(modelID);
        return runner.predictRegression(rawData, schema);
    }

    public static String predictClusterProbabilities(final String modelID, final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException,
            UnsupportedLibraryExcetion, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        AbstractRunner runner = getRunner(modelID);
        return runner.predictClusterProbabilities(rawData, schema);
    }

    public static int predictCluster(final String modelID, final String rawData, final String schema)
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException,
            ClassNotFoundException, UnsupportedLibraryExcetion, PredictException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        AbstractRunner runner = getRunner(modelID);
        return runner.predictCluster(rawData, schema);
    }

    public static double [] predictKeyValue(final String modelID, final String rawData, final String schema) throws PredictException, ClassNotFoundException, SQLException, UnsupportedLibraryExcetion, IOException, JepException, UnsupportedKerasConfigurationException, InvalidKerasConfigurationException {
        AbstractRunner runner = getRunner(modelID);
        return runner.predictKeyValue(rawData, schema, null, null, -1);
    }

    public static Double splitResult(final String str, final int index) {
        final String v = str.split(";")[index];
        return (Double.valueOf(v.split("=")[1]));
    }

    public static int getPrediction(final String str) {
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
        else // this call works even if activation is null
            operationContext = dataSetProcessor.createOperationContext((Activation)null);
        ArrayList<ExecRow> items = new ArrayList<ExecRow>();

        try {

            AbstractRunner runner = getRunner(this.modelID);
            double [] preds = runner.predictKeyValue(this.rawData, this.schema, this.predictCall, this.predictArgs, this.threshold);

            ExecRow valueRow = new ValueRow(preds.length);

            //Loop through the properties and create an array
            for (int i = 0; i < preds.length; i++) {
                valueRow.setColumn(i+1, new SQLDouble(preds[i]));
            }
            items.add(valueRow);
            operationContext.pushScopeForOp("Parse prediction");
        } catch (PredictException e) {
            LOG.error("Unexpected PredictException: " , e);
            e.printStackTrace();
        } catch (SQLException e) {
            LOG.error("Unexpected SQLException: " , e);
        }
        catch (ClassNotFoundException e) {
            LOG.error("Unexpected ClassNotFoundException: " , e);
        }
        catch (Exception e){
            LOG.error("Unexpected Exception: " , e);
        }
        finally {
            operationContext.popScope();
        }
        return dataSetProcessor.createDataSet(items.iterator());
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

    public static DatasetProvider getMLRunner(final String modelCategory, final String modelID, final String rawData, final String schema) {
        return new MLRunner(modelCategory, modelID, rawData, schema);
    }
    public static DatasetProvider getMLRunner(final String modelCategory, final String modelID, final String rawData, final String schema, final String predictCall, final String predictArgs) {
        return new MLRunner(modelCategory, modelID, rawData, schema, predictCall, predictArgs);
    }

    /**
     * MLRunner VTI implementation used for models that return row based values (classification with probabilities, unsupervised models)
     * @param modelCategory The category of the model (classification/unsupervized etc) based on the PySplice defined categories
     * @param modelID
     * @param rawData
     * @param schema
     */
    public MLRunner(final String modelCategory, final String modelID, final String rawData, final String schema){
        this.modelCategory = modelCategory;
        this.modelID = modelID;
        this.rawData = rawData;
        this.schema = schema;
        this.predictCall = null;
        this.predictArgs = null;
        this.threshold = -1;
    }
    // For sklearn
    public MLRunner(final String modelCategory, final String modelID, final String rawData, final String schema,
                    final String predictCall, final String predictArgs){
        this.modelCategory = modelCategory;
        this.modelID = modelID;
        this.rawData = rawData;
        this.schema = schema;
        this.predictCall = predictCall;
        this.predictArgs = predictArgs;
        this.threshold = -1;
    }
    // For Keras
    public MLRunner(final String modelCategory, final String modelID, final String rawData, final String schema,
                    final String threshold){
        this.modelCategory = modelCategory;
        this.modelID = modelID;
        this.rawData = rawData;
        this.schema = schema;
        this.predictCall = null;
        this.predictArgs = null;
        this.threshold = Double.valueOf(threshold);
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        return 1;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return true;
    }
}