package com.lxs.flink.sink.pgsql;

import com.lxs.flink.pgsql.connection.PooledConnectionFactory;
import de.bytefish.pgbulkinsert.IPgBulkInsert;
import de.bytefish.pgbulkinsert.pgsql.processor.BulkProcessor;
import de.bytefish.pgbulkinsert.pgsql.processor.handler.BulkWriteHandler;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.net.URI;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public abstract class BasePostgresSink<TEntity> extends RichSinkFunction<TEntity> {

    private final URI dataBaseUri;

    private final int bulkSize;

    public BasePostgresSink(URI dataBaseUri, int bulkSize) {
        this.dataBaseUri = dataBaseUri;
        this.bulkSize = bulkSize;
    }

    private BulkProcessor<TEntity> processor;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.processor = new BulkProcessor<>(new BulkWriteHandler<>(getBulkInsert(), new PooledConnectionFactory(dataBaseUri)), bulkSize);
    }

    @Override
    public void close() throws Exception {
        processor.close();
    }

    @Override
    public void invoke(TEntity value, Context context) throws Exception {
        processor.add(value);
    }

    protected abstract IPgBulkInsert<TEntity> getBulkInsert();
}
