package com.splicemachine.mlrunner;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;

import hex.genmodel.easy.exception.PredictException;
import jep.*;
import jep.SharedInterpreter;
import jep.JepException;


import static java.nio.ByteBuffer.allocateDirect;

public class SKRunner extends AbstractRunner {

    ByteBuffer model;

    public SKRunner(final Blob modelBlob) throws SQLException, IOException {
        InputStream is = modelBlob.getBinaryStream();
        int fileSize = (int)modelBlob.length();
        final byte[] allBytes = new byte[fileSize];
        is.read(allBytes);
        this.model = allocateDirect(fileSize).put(allBytes);
    }

    @Override
    public String predictClassification(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        return null;
    }

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
            double result = (double) interp.getValue("pred[0]");
            return result;


        } catch (JepException e) {
            e.printStackTrace();
            return -1.0;
        }
    }

    @Override
    public String predictClusterProbabilities(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        return null;
    }

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
                ArrayList<Double> r = (ArrayList<Double>)interp.getValue("preds");
                result = new double[] {r.get(0).doubleValue(), r.get(1).doubleValue()};

            }
            else if(predictCall.equals("predict_proba")) {
                interp.eval("preds = model.predict_proba([X])");
                NDArray<double[]> r = (NDArray<double[]>) interp.getValue("preds[0]");
                result = new double[r.getDimensions()[0] + 1]; // extra element for prediction class
                int index = 0; //index of list
                double c = 0.0; //prediction class
                double max = 0.0;
                for (double p : r.getData()) {
                    result[index + 1] = p;
                    if (p > max) {
                        max = p;
                        c = index;
                    }
                    index++;
                }
                result[0] = c;
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