package com.milansimonovic.jepsen;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;

import java.util.Random;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Demos what happens when the server starts dropping packets for a while.
 */
public class CouchTimeoutTest {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(CouchTimeoutTest.class);

    final String key = "test";
    final Random random = new Random(); //is thread safe
    final AtomicInteger nextValue = new AtomicInteger(1);
    final Cluster cluster;
    final CouchbaseClient client;
    protected final String NODE = "192.168.1.128";
    protected final String BUCKET = "default";
    protected final int timeoutMillis = 10000;

    public CouchTimeoutTest() {
        cluster = new Cluster.Builder().addToLeft(NODE).build();
        client = new CouchbaseClientFactory(NODE, BUCKET, "", timeoutMillis).getClient();
    }

    public static void main(String[] args) {
        //needs to be done BEFORE CouchbaseClient is initialized!
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        new CouchTimeoutTest().run();
    }

    public void run() {
        cluster.getHealthy();
        setEmptyDocument();
        appendToDocument(50);
        cluster.uniDirectionalPartition();
        cluster.scheduleHealAndShutdownIn(15);
        //sleep while the node recovers?
        appendToDocument(150);
        client.shutdown();
        log.info("DONE!");
    }

    /**
     * Tries to append the element N times
     *
     * @param nextVal
     * @param numAttempts
     * @return updated document
     */
    private String addElementToArray(Integer nextVal, int numAttempts) {
        long start = System.currentTimeMillis();
        for (int i = 0; i < numAttempts; i++) {
            try {
                CASValue<Object> casValue = client.gets(key);
                String updatedValue = appendElement(casValue.getValue().toString(), nextVal);
                final CASResponse casResponse = client.cas(key, casValue.getCas(), updatedValue);
                switch (casResponse) {
                    case OK:
                        log.info("added " + nextVal + " in " + (System.currentTimeMillis() - start));
                        return updatedValue;
                    case EXISTS:
                        log.debug("retrying " + nextVal);
                        break;
                    default:
                        log.error("error trying to add " + nextVal + ": " + casResponse);
                        return null;
                }
            } catch (IllegalStateException ex) {
                log.warn("outpacing the network, backing off: " + ex);
                try {
                    Thread.currentThread().sleep(1000 * random.nextInt(5));
                } catch (InterruptedException e) {
                    log.warn("interrupted while sleeping, failed to add " + nextVal, e);
                    Thread.currentThread().interrupt();
                    return null;
                }
            } catch (RuntimeException ex) {
                if (ex.getCause() instanceof TimeoutException) {
                    log.error("timed out adding " + nextVal);
                }
                log.error("error adding " + nextVal, ex);
                return null;
            }
        }
        log.error("failed to append " + nextVal + " in " + numAttempts + " attempts");
        return null;
    }

    private void appendToDocument(int elementsToAdd) {
        for (int i = 0; i < elementsToAdd; i++) {
            addElementToArray(nextValue.getAndIncrement(), 10);
        }
    }

    private String appendElement(String json, Integer nextVal) {
        final JsonParser parser = new JsonParser();
        final JsonElement el = parser.parse(json);
        final JsonArray elements = el.getAsJsonObject().getAsJsonArray("elements");
        elements.add(new JsonPrimitive(nextVal));
        return el.toString();
    }

    private void setEmptyDocument() {
        try {
            if (!client.set(key, "{\"elements\": []}").get()) {
                throw new RuntimeException("failed to reset doc");
            }
        } catch (Exception e) {
            log.error("failed to reset doc: " + e.getMessage());
            throw new ExceptionInInitializerError(e);
        }
    }
}
