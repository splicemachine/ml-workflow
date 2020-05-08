package com.splicemachine.mlrunner;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;

import hex.genmodel.easy.exception.PredictException;
import jep.Jep;
import jep.*;
import jep.SharedInterpreter;

public class SKRunner extends AbstractRunner {

    ByteBuffer model;

    public SKRunner(final ByteBuffer modelBlob) {

        //FIXME: Get into ByteBuffer format
        this.model = modelBlob;
        //DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
    }

    @Override
    public String predictClassification(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
            interp.eval("import pickle");
            interp.set("X", rawData);
            interp.set("jmodel", javaModel);
            interp.eval("X = X.split(',')");
            interp.eval("model = pickle.loads(jmodel)");
            interp.eval("preds = model.predict([X], return_cov=True)");


        } catch (JepException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Double predictRegression(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {

        } catch (JepException e) {
            e.printStackTrace();
        }
    }

    @Override
    public String predictClusterProbabilities(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException {
        return null;
    }

    @Override
    public int predictCluster(String rawData, String schema) throws InvocationTargetException, IllegalAccessException, SQLException, IOException, ClassNotFoundException, PredictException {
        return 0;
    }

    @Override
    public double[] predictKeyValue(String rawData, String schema, String predictCall, String predictArgs) throws PredictException {
        try (SharedInterpreter interp = new SharedInterpreter())
        {
            DirectNDArray<ByteBuffer> javaModel = new DirectNDArray<>(this.model, true);
            interp.eval("import pickle");
            interp.set("X", rawData);
            interp.set("jmodel", javaModel);
            interp.eval("X = X.split(',')");
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
                ArrayList<Double> r = (ArrayList<Double>)interp.getValue("x");
                result = new double[] {r.get(0).doubleValue(), r.get(1).doubleValue()};

            }
            else if(predictCall.equals("predict_proba")){
                interp.eval("preds = model.predict_proba([X])");
                NDArray<double[]> r = (NDArray<double[]>) interp.getValue("preds[0]");
                result = r.getData();
            }
            else{ // transform
                interp.eval("preds = model.transform([X])");
                NDArray<double[]> r = (NDArray<double[]>) interp.getValue("preds[0]");
                result = r.getData();
            }
            return result;



        } catch (JepException e) {
            e.printStackTrace();
        }
    }
}