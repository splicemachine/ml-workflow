package com.splicemachine.mlrunner;
import java.util.*;

import hex.genmodel.algos.deeplearning.DeeplearningMojoModel;
import hex.genmodel.algos.word2vec.WordEmbeddingModel;
import hex.genmodel.easy.RowData;
import hex.genmodel.easy.exception.PredictException;
import hex.genmodel.easy.EasyPredictModelWrapper;
import hex.genmodel.easy.prediction.*;

public class H2ORunner extends AbstractRunner {
    EasyPredictModelWrapper model;

    public H2ORunner(final Object model) {
        this.model = (EasyPredictModelWrapper) model;
    }

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

    @Override
    public String predictClassification(final String rawData, final String schema) throws PredictException {
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
    public Double predictRegression(final String rawData, final String schema) throws PredictException {
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
    public String predictClusterProbabilities(final String rawData, final String schema) {
        return null;
    }

    @Override
    public int predictCluster(final String rawData, final String schema) throws PredictException {
        ClusteringModelPrediction p = null;
        final RowData row = parseDataToFrame(rawData, schema);
        int cluster = -1;
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
    public double[] predictKeyValue(final String rawData, final String schema, final String predictCall, final String predictArgs) throws PredictException {
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
}