package com.splicemachine.fs_functions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import io.airlift.log.Logger;
import org.joda.time.DateTime;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;

/**
 * The <code>TimestampGeneratorVTI</code> VTI function, which returns a series of timestamps
 * between two timestamps, creating a new timestamp value at each interval.
 * <p>The interval time unit can one of the following literals:<ul>
 * <li>SQL_TSI_FRAC_SECOND
 * <li>SQL_TSI_MICROSECOND, FRAC_SECOND
 * <li>SQL_TSI_SECOND
 * <li>SQL_TSI_MINUTE
 * <li>SQL_TSI_HOUR
 * <li>SQL_TSI_DAY
 * <li>SQL_TSI_WEEK
 * <li>SQL_TSI_MONTH
 * <li>SQL_TSI_QUARTER
 * <li>SQL_TSI_YEAR
 * </ul>
 *
 * <p>The SQL syntax is
 *
 * <blockquote>
 *     <code> SELECT *
 *            FROM com.splicemachine.fs_functions.TimestampGenerator
 *            (
 *              <i>start_time</i>,
 *              <i>end_time</i>,
 *              <i>interval_type</i>,
 *              <i>interval_length</i>
 *            )
 *            timevalues( ts_columnname )
 *     </code>
 * </blockquote>
 *
 *
 * <p>Returns a resultset with one timestamp column containing between two timestamps in indicated timestamp
 * interval.
 */
public class TimestampGeneratorVTI implements DatasetProvider, VTICosting{

    // For VTI Implementation
    private final String startTime ;
    private final String endTime ;
    private final int intervalType;
    private final int numberOfUnitsPerInterval;


    //Provide external context which can be carried with the operation
    protected OperationContext operationContext;
    private static final Logger LOG = Logger.get(TimestampGeneratorVTI.class);




    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation spliceOperation, DataSetProcessor dataSetProcessor, ExecRow execRow)  {

        if (spliceOperation != null)
            operationContext = dataSetProcessor.createOperationContext(spliceOperation);
        else // this call works even if activation is null
            operationContext = dataSetProcessor.createOperationContext((Activation) null);
        ArrayList<ExecRow> items = new ArrayList<>();




        try {

            Activation activation = operationContext.getActivation();
            LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
            DataValueFactory dvf=lcc.getDataValueFactory();
            DataValueDescriptor dvdStart = dvf.getTimestamp( new SQLChar(this.startTime));
            DataValueDescriptor dvdEnd = dvf.getTimestamp( new SQLChar(this.endTime));



            Calendar calendar = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            calendar.setTime(dvdStart.getDateTime().toDate());
            end.setTime(dvdEnd.getDateTime().toDate());

            while( calendar.compareTo(end) < 0) {
                ExecRow valueRow = new ValueRow(2);

                //set asof_ts
                valueRow.setColumn(1, dvf.getTimestamp( new SQLTimestamp(new DateTime(calendar.getTimeInMillis()))));

                switch (this.intervalType) {
                    case DateTimeDataValue.FRAC_SECOND_INTERVAL:
                        calendar.add(Calendar.MILLISECOND, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.SECOND_INTERVAL:
                        calendar.add(Calendar.SECOND, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.MINUTE_INTERVAL:
                        calendar.add(Calendar.MINUTE, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.HOUR_INTERVAL:
                        calendar.add(Calendar.HOUR_OF_DAY, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.DAY_INTERVAL:
                        calendar.add(Calendar.DAY_OF_MONTH, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.WEEK_INTERVAL:
                        calendar.add(Calendar.DAY_OF_MONTH, this.numberOfUnitsPerInterval * 7);
                        break;
                    case DateTimeDataValue.MONTH_INTERVAL:
                        calendar.add(Calendar.MONTH, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.QUARTER_INTERVAL:
                        calendar.add(Calendar.MONTH, this.numberOfUnitsPerInterval * 3);
                        break;
                    case DateTimeDataValue.YEAR_INTERVAL:
                        calendar.add(Calendar.YEAR, this.numberOfUnitsPerInterval);
                        break;
                }
                //set until_ts
                valueRow.setColumn(2, dvf.getTimestamp( new SQLTimestamp(new DateTime(calendar.getTimeInMillis()))));
                items.add(valueRow);
            }
            operationContext.pushScopeForOp("Timestamp calculation");
        } catch (Exception e) {
            LOG.error("Unexpected Exception: ", e);
        } finally {
            operationContext.popScope();
        }
        return dataSetProcessor.createDataSet(items.iterator());
    }

    @Override
    public ResultSetMetaData getMetaData()  {

        return null;
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }

    public static DatasetProvider getTimestampGeneratorVTI(final String startTime,
                                                           final String endTime,
                                                           final Integer intervalType,
                                                           final Integer intervalLength) {
        return new TimestampGeneratorVTI( startTime, endTime, intervalType, intervalLength );
    }



    /**
     * TimestampGeneratorVTI VTI implementation
     *
     * @param startTime Start of the timeframe to be split
     * @param endTime End of the timeframe to be split
     * @param intervalType Time units to use for duration of each split
     * @param numberOfUnitsPerInterval size of each split expressed in [intervalType] units
     */
    public TimestampGeneratorVTI(final String startTime, final String endTime,
                                 final Integer intervalType, final Integer numberOfUnitsPerInterval) {

        this.startTime = startTime;
        this.endTime = endTime;
        this.intervalType = intervalType;
        this.numberOfUnitsPerInterval = numberOfUnitsPerInterval;
    }

    public TimestampGeneratorVTI() {
        this.startTime = "1900-01-01 00:00:00";
        this.endTime = "1900-01-01 01:00:00";
        this.intervalType = 1;
        this.numberOfUnitsPerInterval = 60;
    }

    private static long getIntervalMillis(int intervalType){
        long intervalUnitMillis=0;

        switch (intervalType)
        {
            case DateTimeDataValue.FRAC_SECOND_INTERVAL:
                intervalUnitMillis = 1L;
                break;
            case DateTimeDataValue.SECOND_INTERVAL:
                intervalUnitMillis = 1000L;
                break;
            case DateTimeDataValue.MINUTE_INTERVAL:
                intervalUnitMillis = 60000L;
                break;
            case DateTimeDataValue.HOUR_INTERVAL:
                intervalUnitMillis = 3600000L;
                break;
            case DateTimeDataValue.DAY_INTERVAL:
                intervalUnitMillis = 86400000L;
                break;
            case DateTimeDataValue.WEEK_INTERVAL:
                intervalUnitMillis = 604800000L;
                break;
            case DateTimeDataValue.MONTH_INTERVAL:
                intervalUnitMillis = 2592000000L;
                break;
            case DateTimeDataValue.QUARTER_INTERVAL:
                intervalUnitMillis = 7776000000L;
                break;
            case DateTimeDataValue.YEAR_INTERVAL:
                intervalUnitMillis = 31536000000L;
                break;
            default:
                intervalUnitMillis = 1L;
                break;
        }

        return intervalUnitMillis;
    }

    public static Timestamp getSnappedTimestamp( Timestamp sourceTS, int intervalType, int intervalLength)
                                        throws StandardException
    {
        if (sourceTS == null) return null;

        long start_ms = sourceTS.getTime();
        long interval_ms = getIntervalMillis(intervalType) * intervalLength;

        long result=0L;


        if (interval_ms >0 ) {
            result = ((long)(start_ms / interval_ms) * interval_ms);
        }
        return new Timestamp(result);
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {

        if ((this.startTime == null) || (this.endTime==null))
            return 1;

        long start_ms = Timestamp.valueOf(this.startTime).getTime();
        long end_ms = Timestamp.valueOf(this.endTime).getTime();
        long intervalUnitMillis = getIntervalMillis( this.intervalType);

        // if interval is inverted there are no results
        if (end_ms<start_ms) return 0;

        long l = (end_ms - start_ms) / (intervalUnitMillis * this.numberOfUnitsPerInterval);
        return (double)l;
    }

    @Override
    public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment) throws SQLException {
        return 100.0;
    }

    @Override
    public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment) throws SQLException {
        return false;
    }
}




