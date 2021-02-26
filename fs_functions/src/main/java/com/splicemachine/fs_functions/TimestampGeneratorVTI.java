package com.splicemachine.fs_functions;

import java.sql.*;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import com.splicemachine.db.iapi.types.DateTimeDataValue;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Calendar;

import io.airlift.log.Logger;

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
    private java.sql.Timestamp startTime = null;
    private java.sql.Timestamp endTime = null;
    private int intervalType = 0;
    private int numberOfUnitsPerInterval = 0;


    //Provide external context which can be carried with the operation
    protected OperationContext operationContext;
    private static final Logger LOG = Logger.get(TimestampGeneratorVTI.class);

    @Override
    public DataSet<ExecRow> getDataSet(SpliceOperation spliceOperation, DataSetProcessor dataSetProcessor, ExecRow execRow) throws StandardException {

        if (spliceOperation != null)
            operationContext = dataSetProcessor.createOperationContext(spliceOperation);
        else // this call works even if activation is null
            operationContext = dataSetProcessor.createOperationContext((Activation) null);
        ArrayList<ExecRow> items = new ArrayList<ExecRow>();

        try {

            Calendar calendar = Calendar.getInstance();
            Calendar end = Calendar.getInstance();
            calendar.setTime(this.startTime);
            end.setTime(this.endTime);

            while( calendar.compareTo(end) < 0) {
                ExecRow valueRow = new ValueRow(1);

                valueRow.setColumn(1, new SQLTimestamp(new DateTime(calendar.getTimeInMillis())));

                switch (this.intervalType) {
                    case DateTimeDataValue.FRAC_SECOND_INTERVAL:
                        calendar.add(calendar.MILLISECOND, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.SECOND_INTERVAL:
                        calendar.add(calendar.SECOND, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.MINUTE_INTERVAL:
                        calendar.add(calendar.MINUTE, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.HOUR_INTERVAL:
                        calendar.add(calendar.HOUR, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.DAY_INTERVAL:
                        calendar.add(calendar.DAY_OF_MONTH, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.WEEK_INTERVAL:
                        calendar.add(calendar.DAY_OF_MONTH, this.numberOfUnitsPerInterval * 7);
                        break;
                    case DateTimeDataValue.MONTH_INTERVAL:
                        calendar.add(calendar.MONTH, this.numberOfUnitsPerInterval);
                        break;
                    case DateTimeDataValue.QUARTER_INTERVAL:
                        calendar.add(calendar.MONTH, this.numberOfUnitsPerInterval * 3);
                        break;
                    case DateTimeDataValue.YEAR_INTERVAL:
                        calendar.add(calendar.YEAR, this.numberOfUnitsPerInterval);
                        break;
                }
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
    public ResultSetMetaData getMetaData() throws SQLException {

        return null;
    }

    @Override
    public OperationContext getOperationContext() {
        return this.operationContext;
    }

    public static DatasetProvider getTimestampGeneratorVTI(final Object startTime,
                                                           final Object endTime,
                                                           final int intervalType,
                                                           final int intervalLength) {
        return new TimestampGeneratorVTI( startTime, endTime, intervalType, intervalLength );
    }



    /**
     * TimestampGeneratorVTI VTI implementation
     *
     * @param startTime
     * @param endTime
     * @param intervalType
     * @param numberOfUnitsPerInterval
     */
    public TimestampGeneratorVTI(final java.sql.Timestamp startTime, final java.sql.Timestamp endTime,
                                 final int intervalType, final int numberOfUnitsPerInterval) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.intervalType = intervalType;
        this.numberOfUnitsPerInterval = numberOfUnitsPerInterval;
    }

    public TimestampGeneratorVTI(final Object startTime, final Object endTime,
                                 final int intervalType, final int numberOfUnitsPerInterval) {
        LOG.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" + startTime.toString());
        this.startTime = (java.sql.Timestamp)startTime;
        this.endTime = (java.sql.Timestamp)endTime;
        this.intervalType = intervalType;
        this.numberOfUnitsPerInterval = numberOfUnitsPerInterval;
    }

    @Override
    public double getEstimatedRowCount(VTIEnvironment vtiEnvironment) throws SQLException {
        long start_ms = this.startTime.getTime();
        long end_ms = this.endTime.getTime();
        long intervalUnitMillis;

        switch (this.intervalType)
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