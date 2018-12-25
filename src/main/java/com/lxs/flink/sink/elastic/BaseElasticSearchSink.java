package com.lxs.flink.sink.elastic;

import de.bytefish.elasticutils.client.IElasticSearchClient;
import de.bytefish.elasticutils.elasticsearch6.client.ElasticSearchClient;
import de.bytefish.elasticutils.elasticsearch6.client.bulk.configuration.BulkProcessorConfiguration;
import de.bytefish.elasticutils.elasticsearch6.client.bulk.options.BulkProcessingOptions;
import de.bytefish.elasticutils.elasticsearch6.mapping.IElasticSearchMapping;
import de.bytefish.elasticutils.elasticsearch6.utils.ElasticSearchUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/21
 **/
public abstract class BaseElasticSearchSink<TEntity> extends RichSinkFunction<TEntity> {

    private final String host;

    private final int port;

    private final int bulkSize;

    private IElasticSearchClient<TEntity> client;

    public BaseElasticSearchSink(String host, int port, int bulkSize) {
        this.host = host;
        this.port = port;
        this.bulkSize = bulkSize;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.setProperty("es.set.netty.runtime.available.processors", "false");
        Settings settings = Settings.builder()
                .put("cluster.name", "es-cluster")
                .put("client.transport.sniff", true).build();

        TransportClient transportClient = new PreBuiltTransportClient(settings);
        transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host), port));
        List<DiscoveryNode> nodes = transportClient.listedNodes();
        if (nodes.isEmpty()) {
            throw new RuntimeException("Client is not connected to any Elasticsearch nodes!");
        }
        createIndexAndMapping(transportClient);
        BulkProcessingOptions build = BulkProcessingOptions.builder().setBulkActions(bulkSize).build();
        client = new ElasticSearchClient<>(transportClient, getIndexName(), getMapping(), new BulkProcessorConfiguration(build));
    }

    @Override
    public void close() throws Exception {
        if (client != null) {
            client.awaitClose(10, TimeUnit.SECONDS);
        }
    }

    @Override
    public void invoke(TEntity value, Context context) throws Exception {
        client.index(value);
    }

    protected abstract String getIndexName();

    protected abstract IElasticSearchMapping getMapping();

    private void createIndexAndMapping(Client client) {
        createIndex(client, getIndexName());
        createMapping(client, getIndexName(), getMapping());
    }

    private void createIndex(Client client, String indexName) {
        if (!ElasticSearchUtils.indexExist(client, indexName).isExists()) {
            ElasticSearchUtils.createIndex(client, indexName);
        }
    }

    private void createMapping(Client client, String indexName, IElasticSearchMapping mapping) {
        if (ElasticSearchUtils.indexExist(client, indexName).isExists()) {
            ElasticSearchUtils.putMapping(client, indexName, mapping);
        }
    }
}
