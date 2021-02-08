package com.splicemachine.mlrunner;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.*;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.services.io.Formatable;
import hex.genmodel.algos.deeplearning.DeeplearningMojoModel;
import hex.genmodel.algos.word2vec.WordEmbeddingModel;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.*;
import io.airlift.log.Logger;

public class H2ORunner extends AbstractRunner implements Formatable {
    EasyPredictModelWrapper model;

    // For serializing and deserializing across spark
    SQLBlob deserModel;

    private static final Logger LOG = Logger.get(MLRunner.class);

    public H2ORunner(){};

    public H2ORunner(final Blob modelBlob) throws SQLException, IOException, ClassNotFoundException {
        this.deserModel = new SQLBlob(modelBlob);
        final InputStream bis = modelBlob.getBinaryStream();
        final ObjectInputStream ois = new ObjectInputStream(bis);
        this.model = (EasyPredictModelWrapper) ois.readObject();
    }

    @Deprecated
    private RowData parseDataToFrame(final String rawData, final String schema) {
        // Schema parsing setup
        final String[] schemaStrings = schema.split(",");
        // Raw data parse setup
        final String[] splits = rawData.split(",");
        final RowData row = new RowData();
        // Parsing data and creating LeapFrame
        for (int i = 0; i < schemaStrings.length; i++) {
            // get the name and type of the column
            final String schStr = schemaStrings[i].trim();
            final String col = schStr.trim().split(" ")[0];
            // Create the StructField and properly convert the raw value
            row.put(col, splits[i].replace("'", ""));
        }
        // Create our LeapFrame
        return row;
    }

    private List<RowData> parseDataToFrame(LinkedList<ExecRow> unprocessedRows, List<Integer> modelFeaturesIndexes,
                                           List<String> featureColumnNames) throws StandardException {

        // MOJOs don't support "frames", so we'll store it in a list
        List<RowData> frameRows = new ArrayList<>();
        // For each ExecRow, grab all of the model columns and put then in the H2O Row
        Iterator<ExecRow> unpr = unprocessedRows.descendingIterator();
        while(unpr.hasNext()){
            ExecRow dbRow = unpr.next(); // The DB row
            RowData row = new RowData(); // The H2O Row
            for(int ind = 0; ind < featureColumnNames.size(); ind++){
                row.put(featureColumnNames.get(ind), dbRow.getColumn(modelFeaturesIndexes.get(ind)).getString());
            }
            frameRows.add(row);
        }
        return frameRows;
    }

    @Override
    public Queue<ExecRow> predictClassification(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) throws StandardException, PredictException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        final List<RowData> frameRows = parseDataToFrame(rows,modelFeaturesIndexes,featureColumnNames);
        AbstractPrediction p;
        // Get model category
        // Loop through available H2O Rows, Make Prediction, Modify dbRow, add to transformedRows
        switch(model.getModelCategory()){
            case Ordinal:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictOrdinal(rowData);
                    // Set all probabilities and the prediction column
                    int predCol = 0;
                    double maxValue = 0.0;
                    int maxIndex = -1;
                    for(double prob : ((OrdinalModelPrediction)p).classProbabilities) {
                        // Set the row column
                        transformedRow.setColumnValue(predictionLabelIndexes.get(predCol), new SQLDouble(prob));
                        // Check for max prob
                        if(prob > maxValue){
                            maxValue = prob;
                            maxIndex = predCol; // index of the predictionLabelIndexes/predictionLabels lists, not of the row
                        }
                        predCol++;
                    }
                    transformedRow.setColumn(predictionColIndex, new SQLVarchar(predictionLabels.get(maxIndex)));
                    transformedRows.add(transformedRow);
                }
                break;
            case Binomial:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictBinomial(rowData);
                    // Set all probabilities and the prediction column
                    int predCol = 0;
                    double maxValue = 0.0;
                    int maxIndex = -1;
                    for(double prob : ((BinomialModelPrediction)p).classProbabilities) {
                        // Set the row column
                        transformedRow.setColumnValue(predictionLabelIndexes.get(predCol), new SQLDouble(prob));
                        // Check for max prob
                        if(prob > maxValue){
                            maxValue = prob;
                            maxIndex = predCol; // index of the predictionLabelIndexes/predictionLabels lists, not of the row
                        }
                        predCol++;
                    }
                    transformedRow.setColumn(predictionColIndex, new SQLVarchar(predictionLabels.get(maxIndex)));
                    transformedRows.add(transformedRow);
                }
                break;
            case Multinomial:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictMultinomial(rowData);
                    // Set all probabilities and the prediction column
                    int predCol = 0;
                    double maxValue = 0.0;
                    int maxIndex = -1;
                    for(double prob : ((MultinomialModelPrediction)p).classProbabilities) {
                        // Set the row column
                        transformedRow.setColumnValue(predictionLabelIndexes.get(predCol), new SQLDouble(prob));
                        // Check for max prob
                        if(prob > maxValue){
                            maxValue = prob;
                            maxIndex = predCol; // index of the predictionLabelIndexes/predictionLabels lists, not of the row
                        }
                        predCol++;
                    }
                    transformedRow.setColumn(predictionColIndex, new SQLVarchar(predictionLabels.get(maxIndex)));
                    transformedRows.add(transformedRow);
                }
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictRegression(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws StandardException, PredictException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        final List<RowData> frameRows = parseDataToFrame(rows,modelFeaturesIndexes,featureColumnNames);
        RegressionModelPrediction p;
        double value;
        switch(model.getModelCategory()){
            case Regression:
            case HGLMRegression:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictRegression(rowData);
                    value = p.value;
                    transformedRow.setColumnValue(predictionColIndex, new SQLDouble(value));
                    transformedRows.add(transformedRow);
                }
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictClusterProbabilities(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames) {
        return null;
    }

    @Override
    public Queue<ExecRow> predictCluster(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> featureColumnNames) throws StandardException, PredictException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        final List<RowData> frameRows = parseDataToFrame(rows,modelFeaturesIndexes,featureColumnNames);
        ClusteringModelPrediction p;
        int cluster;
        switch(model.getModelCategory()){
            case Clustering:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictClustering(rowData);
                    cluster = p.cluster;
                    transformedRow.setColumnValue(predictionColIndex, new SQLInteger(cluster));
                    transformedRows.add(transformedRow);
                }
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return transformedRows;
    }

    @Override
    public Queue<ExecRow> predictKeyValue(LinkedList<ExecRow> rows, List<Integer> modelFeaturesIndexes, int predictionColIndex, List<String> predictionLabels, List<Integer> predictionLabelIndexes, List<String> featureColumnNames, final String predictCall, final String predictArgs, double threshold) throws PredictException, StandardException {
        Queue<ExecRow> transformedRows = new LinkedList<>();
        final List<RowData> frameRows = parseDataToFrame(rows,modelFeaturesIndexes,featureColumnNames);
        AbstractPrediction p;
        switch(model.getModelCategory()){
            case AutoEncoder:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictAutoEncoder(rowData);

                    // Set the dbRow columns for the predictions
                    int ind = 0;
                    for(double rec : ((AutoEncoderModelPrediction) p).reconstructed){
                        transformedRow.setColumnValue(predictionLabelIndexes.get(ind), new SQLDouble(rec));
                        ind++;
                    }
                    if(model.m instanceof DeeplearningMojoModel){ // This subcategory returns the MSE
                        transformedRow.setColumnValue(predictionLabelIndexes.get(ind), new SQLDouble(((AutoEncoderModelPrediction)p).mse));
                    }
                    transformedRows.add(transformedRow);
                }
                break;
            case WordEmbedding:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictWord2Vec(rowData);
                    // Word2Vec returns a hashmap but we need order, so we also get the list of words in order
                    final HashMap<String, float[]> embeddings = ((Word2VecPrediction) p).wordEmbeddings;
                    for (String word : model.m.getNames()){
                        int ind = 0;
                        for(double vec : embeddings.get(word)) {
                            transformedRow.setColumnValue(predictionLabelIndexes.get(ind), new SQLDouble(vec));
                        }
                        transformedRows.add(transformedRow);
                    }
                }
                break;
            case DimReduction:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictDimReduction(rowData);

                    // Set the dbRow columns for the predictions
                    int ind = 0;
                    for(double rec : ((DimReductionModelPrediction) p).dimensions){
                        transformedRow.setColumnValue(predictionLabelIndexes.get(ind), new SQLDouble(rec));
                        ind++;
                    }
                    transformedRows.add(transformedRow);
                }
                break;
            case AnomalyDetection:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.predictAnomalyDetection(rowData);

                    final double score = ((AnomalyDetectionPrediction) p).score;
                    final double normalizedScore = ((AnomalyDetectionPrediction) p).normalizedScore;

                    transformedRow.setColumnValue(predictionLabelIndexes.get(0), new SQLDouble(score));
                    transformedRow.setColumnValue(predictionLabelIndexes.get(1), new SQLDouble(normalizedScore));
                    transformedRows.add(transformedRow);
                }
                break;
            case TargetEncoder:
                for(RowData rowData : frameRows){
                    ExecRow transformedRow = rows.remove();
                    p = model.transformWithTargetEncoding(rowData);

                    // Set the dbRow columns for the predictions
                    int ind = 0;
                    for(double tf : ((TargetEncoderPrediction) p).transformations){
                        transformedRow.setColumnValue(predictionLabelIndexes.get(ind), new SQLDouble(tf));
                        ind++;
                    }
                    transformedRows.add(transformedRow);
                }
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return transformedRows;
    }



    @Override
    @Deprecated public String predictClassification(final String rawData, final String schema) throws PredictException {
        final RowData row = parseDataToFrame(rawData, schema);
        double [] classProbs = null;
        AbstractPrediction p = null;
        switch(model.getModelCategory()){
            case Ordinal:
                p = model.predictOrdinal(row);
                classProbs = ((OrdinalModelPrediction)p).classProbabilities;
                break;
            case Binomial:
                p = model.predictBinomial(row);
                classProbs = ((BinomialModelPrediction)p).classProbabilities;
                break;
            case Multinomial:
                p = model.predictMultinomial(row);
                classProbs = ((MultinomialModelPrediction)p).classProbabilities;
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        final StringBuilder builder = new StringBuilder();
        for(int i = 0; i < classProbs.length; i++){
            builder.append(i).append("=").append(classProbs[i]).append(";");
        }
        return builder.substring(0, builder.length() - 1);
    }

    @Override
    @Deprecated public Double predictRegression(final String rawData, final String schema) throws PredictException {
        final RowData row = parseDataToFrame(rawData, schema);
        double value = 0.0;
        AbstractPrediction p = null;
        switch(model.getModelCategory()){
            case Regression:
            case HGLMRegression:
                p = model.predictRegression(row);
                value = ((RegressionModelPrediction)p).value;
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return value;
    }

    @Override
    @Deprecated public String predictClusterProbabilities(final String rawData, final String schema) {
        return null;
    }

    @Override
    @Deprecated public int predictCluster(final String rawData, final String schema) throws PredictException {
        ClusteringModelPrediction p;
        final RowData row = parseDataToFrame(rawData, schema);
        int cluster;
        switch(model.getModelCategory()){
            case Clustering:
                p = model.predictClustering(row);
                cluster = ((ClusteringModelPrediction)p).cluster;
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return cluster;
    }

    @Override
    @Deprecated public double[] predictKeyValue(final String rawData, final String schema, final String predictCall, final String predictArgs, double threshold) throws PredictException {
        final RowData row = parseDataToFrame(rawData, schema);
        double [] result;
        AbstractPrediction p = null;
        switch (model.getModelCategory()) {
            case AutoEncoder:
                p = model.predictAutoEncoder(row);
                double [] rowData = ((AutoEncoderModelPrediction) p).reconstructed;
                if(model.m instanceof DeeplearningMojoModel){
                    final Double mse = ((AutoEncoderModelPrediction)p).mse;
                    result = new double[rowData.length + 1];
                    System.arraycopy(rowData, 0, result, 0, rowData.length);
                    result[rowData.length] = mse;
                }
                else{
                    result = rowData;
                }
                break;
            case WordEmbedding:
                p = model.predictWord2Vec(row);
                final HashMap<String, float[]> embeddings = ((Word2VecPrediction) p).wordEmbeddings;
                int vecSize = ((WordEmbeddingModel)model.m).getVecSize();
                result = new double[model.m.getNames().length * vecSize];
                int colIndex = 0;
                for (String col : model.m.getNames()){
                    for (double d : embeddings.get(col)){
                        result[colIndex] = d;
                        colIndex++;
                    }
                }
                break;
            case DimReduction:
                p = model.predictDimReduction(row);
                final double[] dim = ((DimReductionModelPrediction) p).dimensions;
                result = dim;
                break;
            case AnomalyDetection:
                p = model.predictAnomalyDetection(row);
                final double score = ((AnomalyDetectionPrediction) p).score;
                final double normalizedScore = ((AnomalyDetectionPrediction) p).normalizedScore;
                result = new double[] {score, normalizedScore};
                break;
            case TargetEncoder:
                p = model.transformWithTargetEncoding(row);
                final double [] transformations = ((TargetEncoderPrediction) p).transformations;
                result = transformations;
                break;
            case Unknown:
                throw new PredictException("Unknown model category");
            default:
                throw new PredictException("Unhandled model category (" + model.getModelCategory() + ") in switch statement");
        }
        return result;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.deserModel);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        SQLBlob sqlModelBlob = (SQLBlob) in.readObject();
        Blob modelBlob;
        try {
            modelBlob = (Blob) (sqlModelBlob.getObject());
            final InputStream bis = modelBlob.getBinaryStream();
            final ObjectInputStream ois = new ObjectInputStream(bis);
            this.model = (EasyPredictModelWrapper) ois.readObject();
        } catch (StandardException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int getTypeFormatId() {return super.getTypeFormatId();}
}