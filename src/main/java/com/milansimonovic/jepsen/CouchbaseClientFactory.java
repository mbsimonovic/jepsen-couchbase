package com.milansimonovic.jepsen;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import org.apache.log4j.Logger;

import java.net.URI;
import java.util.ArrayList;

/**
 * @author Milan Simonovic <milan.simonovic@imls.uzh.ch>
 */
public class CouchbaseClientFactory {
    private static final Logger log = Logger.getLogger(CouchbaseClientFactory.class);
    private CouchbaseClient client;
    private final String info;

    /**
     * @param nodes
     * @param bucketName
     * @param password
     * @param timeoutMillis
     */
    public CouchbaseClientFactory(String nodes, String bucketName, String password, Integer timeoutMillis) {
        info = "CouchbaseClientFactory{" + nodes + ", " + bucketName + ", " + password + "}";
        log.info(info);
        ArrayList<URI> nodesList = new ArrayList<URI>();
        for (String node : nodes.split(",")) {
            nodesList.add(URI.create("http://" + node + ":8091/pools"));
        }

        CouchbaseConnectionFactoryBuilder builder = new CouchbaseConnectionFactoryBuilder();
        builder.setOpTimeout(timeoutMillis);//2.5 by default
        builder.setObsTimeout(timeoutMillis);
        try {
            client = new CouchbaseClient(builder.buildCouchbaseConnection(nodesList, bucketName, password));
        } catch (Exception e) {
            log.error("Error connecting to Couchbase: ", e);
            throw new RuntimeException(e);
        }
    }

    //    TODO add shutdown hook for client.shutdown() ?
    public CouchbaseClient getClient() {
        return client;
    }

    @Override
    public String toString() {
        return info;
    }
}
