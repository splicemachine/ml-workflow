package com.splicemachine.mlrunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.EngineDriver;
import hex.genmodel.easy.exception.PredictException;

public abstract class AbstractRunner {

    public abstract String predictClassification(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException;
    public abstract Double predictRegression(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException;
    public abstract String predictClusterProbabilities(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException;
    public abstract int predictCluster(final String rawData, final String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException; 
    public abstract double[] predictKeyValue(final String rawData, final String schema) throws PredictException;
    
    public static Object[] getModelBlob(final String modelID) throws SQLException, IOException, ClassNotFoundException {
        final Connection conn = EngineDriver.driver().getInternalConnection();
        final PreparedStatement pstmt = conn.prepareStatement("select model, library from mlmanager.models where RUN_UUID=?");
        pstmt.setString(1, modelID);
        final ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            final Blob blobModel = rs.getBlob(1);
            final String library = rs.getString(2);
            final InputStream bis = blobModel.getBinaryStream();
            final ObjectInputStream ois = new ObjectInputStream(bis);
            final Object modelBlob = ois.readObject();
            ois.close();
            final Object[] obj = { modelBlob, library };
            return obj;
        }
        throw new SQLException("Model not found in Database!");
    }
}