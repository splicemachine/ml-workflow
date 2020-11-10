package com.splicemachine.mlrunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Queue;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import hex.genmodel.easy.exception.PredictException;
import jep.JepException;

public abstract class AbstractRunner {

    public abstract Queue<ExecRow> predictClassification(Queue<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException, PredictException;
    public abstract Queue<ExecRow> predictRegression(Queue<ExecRow> rows, List<Integer> modelFeaturesIndexes,int predictionColIndex, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException, PredictException, SQLException;
    public abstract Queue<ExecRow> predictClusterProbabilities(Queue<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException;
    public abstract Queue<ExecRow> predictCluster(Queue<ExecRow> rows, List<Integer> modelFeaturesIndexes,int predictionColIndex, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException, PredictException;
    public abstract Queue<ExecRow> predictKeyValue(Queue<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, String predictCall, String predictArgs, double threshold) throws StandardException, SQLException, PredictException;

    @Deprecated public abstract String predictClassification(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException;
    @Deprecated public abstract Double predictRegression(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
    @Deprecated public abstract String predictClusterProbabilities(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException;
    @Deprecated public abstract int predictCluster(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
    public abstract double[] predictKeyValue(final String rawData, final String schema, String predictCall, String predictArgs, double threshold) throws PredictException, JepException, SQLException;



    
    public static Object[] getModelBlob(final String modelID) throws SQLException, IOException, ClassNotFoundException {
        final Connection conn = EngineDriver.driver().getInternalConnection();
        final PreparedStatement pstmt = conn.prepareStatement("select database_binary, file_extension from mlmanager.artifacts where DATABASE_BINARY IS NOT NULL AND RUN_UUID=?");
        pstmt.setString(1, modelID);
        final ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            final Blob blobModel = rs.getBlob(1);
            final String library = rs.getString(2);
            final Object[] obj = { blobModel, library };
            return obj;
        }
        throw new SQLException("Model not found in Database!");
    }
}
