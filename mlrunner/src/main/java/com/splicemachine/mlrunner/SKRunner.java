package com.splicemachine.mlrunner;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import hex.genmodel.easy.exception.PredictException;
import io.airlift.log.Logger;
import jep.*;
import jep.SharedInterpreter;
import jep.JepException;

import static java.nio.ByteBuffer.allocateDirect;
public class SKRunner extends AbstractRunner implements Externalizable {

    // For serializing and deserializing across spark
    SQLBlob deserModel;

    ByteBuffer model;
    private static final Logger LOG = Logger.get(MLRunner.class);

    public SKRunner(){};
    public SKRunner(final Blob modelBlob) throws SQLException, IOException {
        this.deserModel = new SQLBlob(modelBlob);
        InputStream is = modelBlob.getBinaryStream();
        int fileSize = (int)modelBlob.length();
        final byte[] allBytes = new byte[fileSize];
        is.read(allBytes);
        this.model = allocateDirect(fileSize).put(allBytes);
    }

    private Object[][] parseData(LinkedList<ExecRow> unprocessedRows, List<Integer> modelFeaturesIndexes) throws StandardException {
        int numCols = modelFeaturesIndexes.size();
        Object [][] rows = new Object[unprocessedRows.size()][numCols];
        // For each ExecRow, grab all of the model columns and put then in the H2O Row
        int rowNum = 0;
        Iterator<ExecRow> unpr = unprocessedRows.descendingIterator();
        while(unpr.hasNext()){
            ExecRow dbRow = unpr.next();
            for(int col = 0; col < numCols; col++){
                //TODO: I'm betting there is a better way to do this
                try{
                    rows[rowNum][col] = Double.parseDouble(dbRow.getColumn(modelFeaturesIndexes.get(col)).getString());
                }
                catch(Exception e){
                    rows[rowNum][col] = dbRow.getColumn(modelFeaturesIndexes.get(col)).getString();
                }
            }
            rowNum++;

        }
        return rows;
    }

    @Override
    public Queue<ExecRow> predictClassification(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException, PredictException {
        return null;
    }

    @Override
    public Queue<ExecRow> predictRegression(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws Exception {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        Object [][] features = parseData(rows, modelFeaturesIndexes);
        try (SharedInterpreter interp = new SharedInterpreter()) {

            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);

            interp.eval("import pickle,re");
            interp.set("X", features);
            interp.set("jmodel", javaModel);

            interp.eval("model = pickle.loads(jmodel)");
            interp.eval("preds = model.predict(X)");


            Object result = ((NDArray<Number[]>)interp.getValue("preds")).getData();

            if(result instanceof double[]){
                for (double p : (double[]) result) {
                    ExecRow transformedRow = rows.remove();
                    transformedRow.setColumnValue(predictionColIndex,new SQLDouble(p));
                    transformedRows.add(transformedRow);
                }
            }
            else if(result instanceof float[]){
                for (float p : (float[]) result) {
                    ExecRow transformedRow = rows.remove();
                    transformedRow.setColumnValue(predictionColIndex,new SQLReal(p));
                    transformedRows.add(transformedRow);
                }
            }

            return transformedRows;
        } catch (JepException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }

    }

    @Override
    public Queue<ExecRow> predictClusterProbabilities(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws IllegalAccessException, StandardException, InvocationTargetException {
        return null;
    }

    @Override
    public Queue<ExecRow> predictCluster(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws Exception {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        Object [][] features = parseData(rows, modelFeaturesIndexes);
        try (SharedInterpreter interp = new SharedInterpreter()) {

            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);

            interp.eval("import pickle,re");
            interp.set("X", features);
            interp.set("jmodel", javaModel);

            interp.eval("model = pickle.loads(jmodel)");
            interp.eval("preds = model.predict(X)");

            NDArray<long[]> preds = (NDArray<long[]>) interp.getValue("preds");

            for (long p : preds.getData()) {
                ExecRow transformedRow = rows.remove();
//                LOG.warn("Setting prediction to " + (int)p);
                transformedRow.setColumnValue(predictionColIndex,new SQLInteger((int) p));
//                LOG.warn("Row has prediction value " + transformedRow.getColumn(predictionColIndex).getInt());
                transformedRows.add(transformedRow);
            }
            return transformedRows;
        } catch (JepException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
    }

    @Override
    public Queue<ExecRow> predictKeyValue(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, String predictCall, String predictArgs, double threshold) throws Exception {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        Object [][] features = parseData(rows, modelFeaturesIndexes);

        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);

            interp.eval("import pickle");
            interp.eval("import re");
            interp.set("X", features);
            interp.set("jmodel", javaModel);

            interp.eval("model = pickle.loads(jmodel)");
            if(predictCall.equals("predict")){
                Object preds;
                Object extras; //covariance or std
                if(predictArgs.equals("return_std")){
                    interp.eval("preds = model.predict([X], return_std=True)");
                    preds = ((NDArray<?>)interp.getValue("preds[0]")).getData();
                    extras = ((NDArray<?>)interp.getValue("preds[1]")).getData();
                }
                else{ //return_cov
                    interp.eval("preds = model.predict(X, return_cov=True)");
                    preds = ((NDArray<?>)interp.getValue("preds[0]")).getData();
                    extras = ((NDArray<?>)interp.getValue("preds[1].diagonal()")).getData();
                }
                // Because Java can't deal with a single structure for floats and doubles, and we don't know what will
                // be returned
                if(preds instanceof double[]) {
                    double [] p = (double[])preds;
                    double [] e = (double[])extras;
                    for(int i = 0; i < p.length; i++) {
                        ExecRow dbRow = rows.remove();
                        dbRow.setColumnValue(predictionColIndex, new SQLDouble(p[i]));
                        dbRow.setColumnValue(predictionColIndex+1, new SQLDouble(e[i]));
                        transformedRows.add(dbRow);
                    }
                }
                else if(preds instanceof float[]){
                    float [] p = (float[])preds;
                    float [] e = (float[])extras;
                    for(int i = 0; i < p.length; i++) {
                        ExecRow dbRow = rows.remove();
                        dbRow.setColumnValue(predictionColIndex, new SQLReal(p[i]));
                        dbRow.setColumnValue(predictionColIndex+1, new SQLReal(e[i]));
                        transformedRows.add(dbRow);
                    }
                }
                else {throw new Exception("The datatype of the output was not float or double!");}
            }
            else if(predictCall.equals("predict_proba")) {

                interp.eval("preds = model.predict_proba(X)");
                Object result = ((NDArray<?>) interp.getValue("preds")).getData();
                if(!(result instanceof double[])){
                    throw new Exception("Your model is returning Java floats instead of Doubles! Use a Pipeline to cast" +
                            "the output to a double: ie `('postprocessor', FunctionTransformer(lambda X: X.astype('double'), validate=True)`");
                }
                double[] res = (double[])result;
                LOG.warn("we have " + res.length + " values from this batch");
                int numRows = res.length/predictionLabels.size();
                LOG.warn("we have " + numRows + " rows from this batch");
                for(int row=0; row < numRows; row ++){
                    ExecRow dbRow = rows.remove();
                    double max = 0.0;
                    int maxIndex = 0;
                    for(int col=0; col < predictionLabels.size(); col++){
                        double v = res[row+col];
                        dbRow.setColumnValue(predictionLabelIndexes.get(col), new SQLDouble(v));
                        if(v>max){
                            max=v;
                            maxIndex=col;
                        }
                    }
                    dbRow.setColumnValue(predictionColIndex, new SQLVarchar(predictionLabels.get(maxIndex)));
                    transformedRows.add(dbRow);
                }
            }
            else { // transform
                interp.eval("preds = model.transform(X)");
                // We don't know if this will be a double or float so we need to be careful
                Object result = ((NDArray<?>) interp.getValue("preds")).getData();
                if(!(result instanceof double[])){
                    throw new Exception("Your model is returning Java floats instead of Doubles! Use a Pipeline to cast" +
                            "the output to a double: ie `('postprocessor', FunctionTransformer(lambda X: X.astype('double'), validate=True)`");
                }
                double[] res = (double[])result;
                int numRows = res.length/predictionLabels.size();
                for(int row=0; row < numRows; row ++){
                    ExecRow dbRow = rows.remove();
                    for(int col=0; col < predictionLabels.size(); col++){
                        double v = res[row+col];
                        dbRow.setColumnValue(predictionLabelIndexes.get(col), new SQLDouble(v));
                    }
                    transformedRows.add(dbRow);
                }
            }
            return transformedRows;
        }
        catch (JepException e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
            //FIXME: Not sure what exception to throw
        }
    }

    //////////////////////////////////////////////
    //
    // FORMATABLE
    // Jep model class ByteBuffer does not implement serializable so we need to implement it on the SQLBlob (which does)
    //
    //////////////////////////////////////////////

    /** @exception  IOException thrown on error */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(deserModel);
    }

    /**
     * @see java.io.Externalizable#readExternal
     *
     * @exception IOException on error
     * @exception ClassNotFoundException on error
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SQLBlob sqlModelBlob = (SQLBlob) in.readObject();
        Blob modelBlob;
        InputStream is = null;
        int fileSize = 0;
        try {
            modelBlob = (Blob) (sqlModelBlob.getObject());
            is = modelBlob.getBinaryStream();
            fileSize = (int)modelBlob.length();

        } catch (StandardException e) {
            e.printStackTrace();
        }
        catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        final byte[] allBytes = new byte[fileSize];
        is.read(allBytes);
        this.model = allocateDirect(fileSize).put(allBytes);
    }

    @Deprecated
    @Override
    public String predictClassification(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        return null;
    }
    @Deprecated
    @Override
    public Double predictRegression(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
            interp.eval("import pickle");
            interp.eval("import re");
            interp.set("X", rawData);
            interp.set("jmodel", javaModel);
            interp.eval("pat = re.compile('^((?!-0?(\\.0+)?(e|$))-?(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)(e-?(0|[1-9]\\d*))?|0x[0-9a-f]+)$')");
            interp.eval("X = [float(x) if pat.match(x) else x for x in X.split(',')]");
            interp.eval("model = pickle.loads(jmodel)");
            interp.eval("pred = model.predict([X])");
            double result = ((Number) interp.getValue("pred[0]")).doubleValue();
            return result;


        } catch (JepException e) {
            e.printStackTrace();
            return -1.0;
        }
    }

    @Deprecated
    @Override
    public String predictClusterProbabilities(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        return null;
    }
    @Deprecated
    @Override
    public int predictCluster(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException, JepException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
            interp.eval("import pickle");
            interp.eval("import re");
            interp.set("X", rawData);
            interp.set("jmodel", javaModel);
            interp.eval("pat = re.compile('^((?!-0?(\\.0+)?(e|$))-?(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)(e-?(0|[1-9]\\d*))?|0x[0-9a-f]+)$')");
            interp.eval("X = [float(x) if pat.match(x) else x for x in X.split(',')]");
            interp.eval("model = pickle.loads(jmodel)");
            interp.eval("pred = model.predict([X])");
            long l = (long) interp.getValue("pred[0]");
            int result = (int) l;
            return result;

        } catch (JepException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Deprecated
    @Override
    public double[] predictKeyValue(String rawData, String schema, String predictCall, String predictArgs, double threshold) throws PredictException, JepException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
            interp.eval("import pickle");
            interp.eval("import re");
            interp.set("X", rawData);
            interp.set("jmodel", javaModel);
            interp.eval("pat = re.compile('^((?!-0?(\\.0+)?(e|$))-?(0|[1-9]\\d*)?(\\.\\d+)?(?<=\\d)(e-?(0|[1-9]\\d*))?|0x[0-9a-f]+)$')");
            interp.eval("X = [float(x) if pat.match(x) else x for x in X.split(',')]");
            interp.eval("model = pickle.loads(jmodel)");
            double[] result;
            if(predictCall.equals("predict")){
                if(predictArgs.equals("return_std")){
                    interp.eval("preds = model.predict([X], return_std=True)");
                    interp.eval("preds = [preds[0][0], preds[1][0]]");
                }
                else{
                    interp.eval("preds = model.predict([X], return_cov=True)");
                    interp.eval("preds = [preds[0][0], preds[1][0][0]]");
                }
                ArrayList<Number> r = (ArrayList<Number>)interp.getValue("preds");
                result = new double[] {r.get(0).doubleValue(), r.get(1).doubleValue()};

            }
            else if(predictCall.equals("predict_proba")) {
                interp.eval("pred = model.predict([X])");
                Number predClass = (Number) interp.getValue("pred[0]");
                interp.eval("preds = model.predict_proba([X])");
                NDArray<double[]> r = (NDArray<double[]>) interp.getValue("preds[0]");
                result = new double[r.getDimensions()[0] + 1]; // extra element for prediction class
                int index = 0; //index of list
                double max = 0.0;
                for (double p : r.getData()) {
                    result[index + 1] = p;
                    if (p > max) {
                        max = p;
                    }
                    index++;
                }
                result[0] = predClass.doubleValue();
            }
            else{ // transform
                interp.eval("preds = model.transform([X])");
                NDArray<double[]> r = (NDArray<double[]>) interp.getValue("preds[0]");
                result = r.getData();
            }
            return result;

        } catch (JepException e) {
            e.printStackTrace();
            return null; //FIXME: Not sure what exception to throw
        }
    }


}