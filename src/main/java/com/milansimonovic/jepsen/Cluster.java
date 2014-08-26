package com.milansimonovic.jepsen;

import java.util.*;

/**
 * @author Milan Simonovic <milan.simonovic@imls.uzh.ch>
 */
public class Cluster {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(Cluster.class);

    final List<RemoteConnection> rightHemisphere;
    final List<RemoteConnection> leftHemisphere;

    public static void main(String[] args) {
        Cluster c = new Cluster();
        c.getHealthy();
        c.uniDirectionalPartition();
        c.scheduleHealAndCloseConnectionIn(1);
    }

    static class Builder {
        final List<RemoteConnection> rightHemisphere = new ArrayList<>();
        final List<RemoteConnection> leftHemisphere = new ArrayList<>();

        public Builder addToLeft(String node) {
            leftHemisphere.add(new RemoteConnection(node));
            return this;
        }

        public Builder addToRight(String node) {
            rightHemisphere.add(new RemoteConnection(node));
            return this;
        }

        public Cluster build() {
            return new Cluster(leftHemisphere, rightHemisphere);
        }
    }

    public Cluster(List<RemoteConnection> leftHemisphere, List<RemoteConnection> rightHemisphere) {
        this.leftHemisphere = Collections.unmodifiableList(leftHemisphere);
        this.rightHemisphere = Collections.unmodifiableList(rightHemisphere);
    }

    public Cluster() {
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                shutdown();
//            }
//        }); //
        this(Collections.unmodifiableList(Arrays.asList(new RemoteConnection("n1"), new RemoteConnection("n2"))),
                Collections.unmodifiableList(Arrays.asList(new RemoteConnection("n3"), new RemoteConnection("n4"), new RemoteConnection("n5"))));
    }

    public void closeConnection() {
        log.info("closing connections");
        for (RemoteConnection n : leftHemisphere) {
            try {
                n.shutdown();
            } catch (Exception e) {
                log.error("failed to shutdown " + n, e);
            }
        }
        for (RemoteConnection n : rightHemisphere) {
            try {
                n.shutdown();
            } catch (Exception e) {
                log.error("failed to shutdown " + n, e);
            }
        }

    }

    public void uniDirectionalPartition() {
        log.info("creating 1 way partition");
        for (RemoteConnection n : leftHemisphere) {
            n.dropOnPortRange(200, 65535);
        }
        log.info("creating partition DONE");
    }

    public void getHealthy() {
        log.info("healing partition");
        for (RemoteConnection n : leftHemisphere) {
            n.flushIpTables();
        }
        log.info("healing partition - DONE");
    }

    public void scheduleHealAndCloseConnectionIn(int seconds) {
        final Timer timer = new Timer("healer", true);
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                getHealthy();
                closeConnection();
            }
        }, seconds * 1000);
        log.info("scheduled heal in " + seconds + " seconds");

    }
}
