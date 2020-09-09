package com.splicemachine.mlrunner;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.EngineDriver;
import hex.genmodel.easy.exception.PredictException;
import jep.JepException;

public abstract class AbstractRunner {

    public abstract String predictClassification(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException;
    public abstract Double predictRegression(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
    public abstract String predictClusterProbabilities(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException;
    public abstract int predictCluster(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException;
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
