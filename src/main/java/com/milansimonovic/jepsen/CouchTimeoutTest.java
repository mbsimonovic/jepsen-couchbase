package com.milansimonovic.jepsen;

import com.couchbase.client.CouchbaseClient;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
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
    final String NODE = "192.168.1.128";
    final String BUCKET = "default";
    final int timeoutMillis = 10000;
    final int numAttempts = 10;
    int total;
    protected final Set<Integer> acknowledgedWrites = Collections.synchronizedSet(new HashSet<Integer>(200));

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
        cluster.scheduleHealAndCloseConnectionIn(15);
        appendToDocument(150);
        collectResults();
        client.shutdown();
        log.info("DONE!");
    }

    private void collectResults() {
        CASValue<Object> casValue = client.gets(key);
        final JsonElement el = new JsonParser().parse(casValue.getValue().toString());
        final JsonArray elements = el.getAsJsonObject().getAsJsonArray("elements");
        Set<Integer> saved = new HashSet<>();
        for (int i = 0; i < elements.size(); i++) {
            saved.add(elements.get(i).getAsInt());
        }
        Set<Integer> survivors = new HashSet<>(acknowledgedWrites);
        survivors.retainAll(saved);
        Set<Integer> lost = new HashSet<>(acknowledgedWrites);
        lost.removeAll(survivors);
        Set<Integer> unacked = new HashSet<>(saved);
        unacked.removeAll(acknowledgedWrites);

        log.info(total + " total");
        log.info(acknowledgedWrites.size() + " acknowledged");
        log.info(survivors.size() + " survivors");
        if (!lost.isEmpty()) {
            log.info(lost.size() + " acknowledged writes lost!");
            log.info(lost);
        }
        if (!unacked.isEmpty()) {
            log.info(unacked.size() + " unacknowledged writes found!");
            log.info(unacked);
        }
        log.info((double) acknowledgedWrites.size() / total + " ack rate");
        if (!lost.isEmpty()) {
            log.info((double) lost.size() / acknowledgedWrites.size() + " loss rate");
        }
//        if (!unacked.isEmpty()) {
        //this rate doesn't seem correct
//            log.info((double)unacked.size() / acknowledgedWrites.size() + " unacknowledged but successful rate" );
//        }
    }

    /**
     * Tries to append <code>nextVal</code> <code>numAttempts</code> times
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
                        acknowledgedWrites.add(nextVal);
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
                } else {
                    log.error("error trying to add " + nextVal, ex);
                }
            }
        }
        log.error("failed to append " + nextVal + " in " + numAttempts + " attempts");
        return null;
    }

    private void appendToDocument(int elementsToAdd) {
        for (int i = 0; i < elementsToAdd; i++) {
            addElementToArray(nextValue.getAndIncrement(), numAttempts);
        }
        total += elementsToAdd;
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
