package com.cobub.es.common;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class ClientFactory {

    private Client client = null;

    public static Client getClient() {
        Client client = null;
        try {

            client = TransportClient.builder()
                    .settings(
                            Settings.builder()
                                    .put("cluster.name", "cobub-es-cluster")
                                    .put("client.transport.sniff", true) // 设置client.transport.sniff为true来使客户端去嗅探整个集群的状态，把集群中其它机器的ip地址加到客户端中
                    ).build()
                    .addTransportAddresses(
                            new InetSocketTransportAddress(InetAddress.getByName("master001"), 9300)
//                            , new InetSocketTransportAddress(InetAddress.getByName("web"), 9300)
                    )
            ;

//            client = TransportClient.builder().settings(Settings.builder().put("cluster.name", "elasticsearch")).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            System.out.println("unkown host: " + e.getMessage());
        } finally {

            return client;
        }
    }

    public static Client getClientByNode() {
        Node node = NodeBuilder.nodeBuilder().clusterName("cobub-es-cluster")
                .settings(Settings.builder().put("cluster.name", "cobub-es-cluster"))
                .node();
        Client client1 = node.client();
        return client1;
    }

    public void close() {
        if (null != client) {
            client.close();
        }
    }

}
