package com.splicemachine.mlrunner;

import hex.genmodel.easy.exception.PredictException;
import jep.JepException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.SQLException;

import org.deeplearning4j.nn.modelimport.keras.KerasModelImport;
import org.deeplearning4j.nn.multilayer.MultiLayerNetwork;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;
import org.nd4j.autodiff.samediff.SameDiff;
import org.nd4j.imports.graphmapper.tf.TFGraphMapper;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.autodiff.execution.NativeGraphExecutioner;

import org.deeplearning4j.nn.modelimport.keras.*;
import org.deeplearning4j.nn.modelimport.keras.utils.*;
import org.deeplearning4j.util.ModelSerializer;
import org.deeplearning4j.nn.graph.ComputationGraph;
import org.deeplearning4j.nn.modelimport.keras.exceptions.*;

import java.io.*;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.nio.file.*;
import java.util.List;

public class KerasRunner extends AbstractRunner {
    MultiLayerNetwork model;
    public KerasRunner(Blob modelBlob) throws SQLException, UnsupportedKerasConfigurationException, IOException, InvalidKerasConfigurationException {
        InputStream is = modelBlob.getBinaryStream();
        this.model = KerasModelImport.importKerasSequentialModelAndWeights(is, true);
    }

    private INDArray parseDataToArray(String rawData) throws SQLException {
        final String[] rawDatas = rawData.split(",");
        int numFeatures = rawDatas.length;
        // Assuming a 1 dimensional input for models (No CNNs or RNNs for now)
        INDArray features = Nd4j.zeros(1,numFeatures);
        try {
            for (int i = 0; i < numFeatures; i++) {
                features.putScalar(0, i, Double.valueOf(rawDatas[i]));
            }
        }
        catch(Exception e){
            throw new SQLException("Expected input to be of type Double but wasn't\n" , e);
        }
        return features;
    }

    @Override
    public Double predictRegression(String rawData, String schema) throws SQLException, IOException {
        INDArray features = parseDataToArray(rawData);
        Double pred = model.output(features).getDouble();
        return pred;
    }

    @Override
    public double[] predictKeyValue(String rawData, String schema, String predictCall, String predictArgs, double threshold) throws PredictException, JepException, SQLException {
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
            result[0] = output.argMax(1).getDouble();;
            System.arraycopy(output.toDoubleVector(),0,result,1,numClasses);
        }
        return result;
    }


    @Override
    public String predictClusterProbabilities(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        return null;
    }
    @Override
    public int predictCluster(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException {
        return 0;
    }
    @Override
    public String predictClassification(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        return null;
    }
}
