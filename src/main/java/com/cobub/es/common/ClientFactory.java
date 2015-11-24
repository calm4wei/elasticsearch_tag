package com.cobub.es.common;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by feng.wei on 2015/11/13.
 */
public class ClientFactory {

    private  Client client = null;

    public static Client getClient() {
        Client client = null;
        try {

            client = TransportClient.builder().settings(Settings.builder().put("cluster.name", "cobub-es-testing")).build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("master001"), 9300));

//            client = TransportClient.builder().settings(Settings.builder().put("cluster.name", "elasticsearch")).build()
//                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch(UnknownHostException e){
            System.out.println("unkown host: " +e.getMessage());
        } finally {

            return client;
        }
    }

    public void close(){
        if (null != client){
            client.close();
        }
    }
}
