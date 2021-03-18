package com.splicemachine.mlrunner;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLVarchar;

import java.io.*;
import java.sql.Blob;
import java.sql.SQLException;

import io.airlift.log.Logger;
import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import org.deeplearning4j.nn.modelimport.keras.exceptions.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class KerasRunner extends AbstractRunner implements Externalizable {
    private static final Logger LOG = Logger.get(KerasRunner.class);

    MultiLayerNetwork model;

    // For serializing and deserializing across spark
    SQLBlob deserModel;

    public KerasRunner(){};
    public KerasRunner(Blob modelBlob) throws SQLException, UnsupportedKerasConfigurationException, IOException, InvalidKerasConfigurationException {
        this.deserModel = new SQLBlob(modelBlob);
        InputStream is = modelBlob.getBinaryStream();
        this.model = KerasModelImport.importKerasSequentialModelAndWeights(is, true);
    }

    @Deprecated
    private INDArray parseDataToArray(String rawData) throws SQLException {
        final String[] rawDatas = rawData.split(",");
        int numFeatures = rawDatas.length;
        // Assuming a 1 dimensional input for models (No CNNs or RNNs for now)
        INDArray features = Nd4j.zeros(1,numFeatures);
        try {
            for (int i = 0; i < numFeatures; i++) {
                features.putScalar(0, i, Double.parseDouble(rawDatas[i]));
            }
        }
        catch(Exception e){
            throw new SQLException("Expected input to be of type Double but wasn't\n" , e);
        }
        return features;
    }

    private INDArray parseDataToArray(LinkedList<ExecRow> unprocessedRows, List<Integer> modelFeaturesIndexes,
                                     List<String> featureColumnNames) throws SQLException {
        // Create array that is numRows X numFeaturesPerRow
        assert unprocessedRows.peek() != null: "There are no rows in the Queue!";
        INDArray features = Nd4j.zeros(unprocessedRows.size(), modelFeaturesIndexes.size());
        LOG.info("In parse!");
        LOG.info("Size of unprocessed rows: " + unprocessedRows.size());
        LOG.info("Number of features: " + unprocessedRows.peek().nColumns());
        LOG.info("Model feature indexes size: " + modelFeaturesIndexes.size());
        int rowNum = 0;
        try {
            Iterator<ExecRow> unpr = unprocessedRows.descendingIterator();
            while(unpr.hasNext()){
                ExecRow row = unpr.next();
                for (int ind = 0; ind < modelFeaturesIndexes.size(); ind++) {
                    LOG.info("Adding index " + ind);
                    features.putScalar(rowNum, ind, row.getColumn(modelFeaturesIndexes.get(ind)).getDouble());
                }
                rowNum++;
            }
        }
        catch(Exception e){
            throw new SQLException("Expected input to be of type Double but wasn't\n" , e);
        }
        LOG.info("shape of features: " + features.shapeInfoToString());
        LOG.info("number of columns: " + features.columns());
        return features;
    }

    @Override
    public Queue<ExecRow> predictClassification(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) {
        return null;
    }

    @Override
    public Queue<ExecRow> predictRegression(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws StandardException, SQLException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        INDArray features = parseDataToArray(rows, modelFeaturesIndexes, featureColumnNames);
        INDArray output = model.output(features);
        int ind = 0;
        for(ExecRow transformedRow : rows) {
            transformedRow.setColumnValue(predictionColIndex, new SQLDouble(output.getRow(ind).getDouble()));
            ind++;
            transformedRows.add(transformedRow);
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictClusterProbabilities(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) {
        return null;
    }

    @Override
    public Queue<ExecRow> predictCluster(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) {
        return null;
    }

    @Override
    public Queue<ExecRow> predictKeyValue(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, String predictCall, String predictArgs, double threshold) throws StandardException, SQLException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        INDArray features = parseDataToArray(rows, modelFeaturesIndexes, featureColumnNames);
        INDArray output = model.output(features);
        int rowNum = 0;

        if(threshold != -1) {
            for (ExecRow transformedRow : rows) { // DB Row
                INDArray ndRow = output.getRow(rowNum); // Keras NN Row
                double rawOut = ndRow.getDouble();
                int classPred = rawOut > threshold ? 1 : 0;
                transformedRow.setColumnValue(predictionColIndex, new SQLVarchar(predictionLabels.get(classPred)));
                // if prediction is 0.8 and the threshold is 0.5, then prediction = class 1. So class 0 is (1-0.8) = 0.2
                transformedRow.setColumnValue(predictionLabelIndexes.get(0), new SQLDouble(1-rawOut)); // 0 class
                transformedRow.setColumnValue(predictionLabelIndexes.get(1), new SQLDouble(rawOut));       // 1 class
                transformedRows.add(transformedRow);
            }
        }
        else{
            for (ExecRow transformedRow : rows) { // DB Row
                INDArray ndRow = output.getRow(rowNum); // Keras NN Row
                int pred = (int) ndRow.argMax(1).getDouble();
                for (int colNum = 0; colNum < predictionLabelIndexes.size(); colNum++) {
                    transformedRow.setColumnValue(predictionLabelIndexes.get(colNum), new SQLDouble(ndRow.getDouble(colNum)));
                }
                transformedRow.setColumnValue(predictionColIndex, new SQLVarchar(predictionLabels.get(pred)));
                transformedRows.add(transformedRow);
                rowNum++;
            }
        }
        return transformedRows;
    }



    @Override
    public String predictClassification(String rawData, String schema) {
        return null;
    }
    @Override
    public Double predictRegression(String rawData, String schema) throws SQLException {
        INDArray features = parseDataToArray(rawData);
        Double pred = model.output(features).getDouble();
        return pred;
    }

    @Override
    public double[] predictKeyValue(String rawData, String schema, String predictCall, String predictArgs, double threshold) throws SQLException {
        INDArray features = parseDataToArray(rawData);
        INDArray output = model.output(features);
        double [] result;
        // Binary Classification with custom threshold
        if(threshold != -1){
            double classPred = output.getDouble() > threshold ? 1.0 : 0.0;
            result = new double[]{classPred, output.getDouble()};
        }
        // MultiClass problem
        else{
            // Assuming a 1 dimensional output (No CNNs or RNNs for now)
            int numClasses = (int) output.length();
            result = new double[numClasses+1];
            result[0] = output.argMax(1).getDouble();
            System.arraycopy(output.toDoubleVector(),0,result,1,numClasses);
        }
        return result;
    }


    @Override
    public String predictClusterProbabilities(String rawData, String schema) {
        return null;
    }
    @Override
    public int predictCluster(String rawData, String schema) {
        return 0;
    }

    /** @exception  IOException thrown on error */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(deserModel);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SQLBlob sqlModelBlob = (SQLBlob) in.readObject();
        Blob modelBlob;
        InputStream is;
        try {
            modelBlob = (Blob) (sqlModelBlob.getObject());
            is = modelBlob.getBinaryStream();
            this.model = KerasModelImport.importKerasSequentialModelAndWeights(is, true);
        } catch (StandardException | SQLException | InvalidKerasConfigurationException | UnsupportedKerasConfigurationException e) {
            e.printStackTrace();
        }
    }

}
