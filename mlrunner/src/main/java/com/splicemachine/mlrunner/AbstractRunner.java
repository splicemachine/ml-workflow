package com.splicemachine.mlrunner;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import hex.genmodel.easy.exception.PredictException;
import io.airlift.log.Logger;
import jep.JepException;

public abstract class AbstractRunner implements Externalizable {

    private static final Logger LOG = Logger.get(MLRunner.class);

    public abstract Queue<ExecRow> predictClassification(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException, PredictException;
    public abstract Queue<ExecRow> predictRegression(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws Exception;
    public abstract Queue<ExecRow> predictClusterProbabilities(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException;
    public abstract Queue<ExecRow> predictCluster(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws Exception;
    public abstract Queue<ExecRow> predictKeyValue(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, String predictCall, String predictArgs, double threshold) throws Exception;

    @Deprecated public abstract String predictClassification(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException;
    @Deprecated public abstract Double predictRegression(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
    @Deprecated public abstract String predictClusterProbabilities(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException;
    @Deprecated public abstract int predictCluster(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
    public abstract double[] predictKeyValue(final String rawData, final String schema, String predictCall, String predictArgs, double threshold) throws PredictException, JepException, SQLException;
    

      // FIXME: Trying Jun's fix below
    public static Object[] getModelBlob(final String modelID) throws SQLException {
        Connection conn = EngineDriver.driver().getInternalConnection();
        java.sql.PreparedStatement pstmt = conn.prepareStatement("select database_binary, file_extension from mlmanager.artifacts where DATABASE_BINARY IS NOT NULL AND RUN_UUID=?");
        pstmt.setString(1, modelID);
        Object [] obj = null;
        java.sql.ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            final Blob blobModel = rs.getBlob(1);
            final String library = rs.getString(2);
            obj = new Object[]{blobModel, library};
        }
        if(obj == null){
            throw new SQLException("Model not found in Database!");
        }
        else{
            return obj;
        }
    }

//    public static Object[] getModelBlob(final String modelID) throws SQLException {
//        try(Connection conn = DriverManager.getConnection("jdbc:default:connection");){
//            java.sql.PreparedStatement pstmt = conn.prepareStatement("select database_binary, file_extension from mlmanager.artifacts where DATABASE_BINARY IS NOT NULL AND RUN_UUID=?");
//            pstmt.setString(1, modelID);
//            java.sql.ResultSet rs = pstmt.executeQuery();
//            Object [] obj = null;
//            if (rs.next()) {
//                final Blob blobModel = rs.getBlob(1);
//                final String library = rs.getString(2);
//                obj = new Object[]{blobModel, library};
//            }
//            if(obj == null){
//                throw new SQLException("Model not found in Database!");
//            }
//            else{
//                return obj;
//            }
//        }
//    }

//    public static Object[] getModelBlob(final String modelID, LanguageConnectionContext languageConnectionContext) throws SQLException, StandardException {
//        final String sql = String.format("select database_binary, file_extension from mlmanager.artifacts where DATABASE_BINARY IS NOT NULL AND RUN_UUID='%s'",modelID);
//        final PreparedStatement pstmt;
//        if (languageConnectionContext == null) {
//            throw new SQLException("Missing a languageConnectionContext!");
////            final Connection conn = EngineDriver.driver().getInternalConnection();
////            conn.setAutoCommit(false);
////            final PreparedStatement pstmt = conn.prepareStatement(sql);
//        }
//        else{
//            pstmt = languageConnectionContext.prepareInternalStatement(sql);
//        }
//        Activation activation = pstmt.getActivation(languageConnectionContext, false);
//        ((GenericStorablePreparedStatement)pstmt).setNeedsSavepoint(false);
//        ResultSet rs = pstmt.execute(activation, 0);
//        ExecRow row = rs.getNextRow();
//        if (row != null) {
//            LOG.info("Row has data!");
//
//            LOG.info("Row has contents: " + row.toString());
//            LOG.info("Column 1 has contents: " + row.getColumn(1).toString());
//            LOG.info("Column 2 has contents: " + row.getColumn(2).toString());
//
//            final Blob blobModel = (Blob) row.getColumn(1).getObject();
//            LOG.info("blob model object is null: " + (blobModel==null));
//            final String library = row.getColumn(2).getString();
//            LOG.info("Library version is null: " + (library==null));
//            LOG.info("Library is " + library);
//            final Object[] obj = {blobModel, library};
//            return obj;
//        }
//        throw new SQLException("Model not found in Database!");
//    }


    @Override
    public abstract void writeExternal(ObjectOutput out) throws IOException;

    @Override
    public abstract void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;
}
