package com.milansimonovic.jepsen;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author Milan Simonovic <milan.simonovic@imls.uzh.ch>
 */
public class RemoteConnection {
    private static final org.apache.log4j.Logger log = org.apache.log4j.Logger.getLogger(RemoteConnection.class);

    final JSch jsch = new JSch();
    final String flushIptables = "sudo iptables -F";
    final String dropOnPortRangeFormat = "sudo iptables -A INPUT -p tcp --dport %d:%d -j DROP";
    private final String host;
    protected Session session;

    public RemoteConnection(String host) {
        log.info(host);
        this.host = host;
        try {
            jsch.addIdentity("~/.ssh/id_rsa", "");
        } catch (JSchException e) {
            log.error("failed to add key", e);
            throw new ExceptionInInitializerError(e);
        }
        try {
            jsch.setKnownHosts("~/.ssh/known_hosts");
        } catch (JSchException e) {
            log.error("failed to add known hosts", e);
            throw new ExceptionInInitializerError(e);
        }
        try {
            session = jsch.getSession("server-adm", host, 22);
        } catch (JSchException e) {
            log.error("failed to get session", e);
            throw new ExceptionInInitializerError(e);
        }
        try {
            session.connect(1000);
        } catch (JSchException e) {
            log.error("failed to connect", e);
            throw new ExceptionInInitializerError(e);
        }
    }

    public void flushIpTables() {
        exec(flushIptables);
    }

    public void dropOnPortRange(Integer from, Integer to) {
        exec(String.format(dropOnPortRangeFormat, from, to));
    }

    protected String exec(String command) {
        final ChannelExec channel = openChannel(command);
        final String res;
        try {
            final InputStream in = getInputStream(channel);
            connect(channel);
            res = readOutput(command, channel, in);
        } finally {
            try {
                if (channel.isConnected()) channel.disconnect();
            } catch (Exception e) {
                log.warn("failed to disconnect channel", e);
            }
        }
        return res;
    }

    public void shutdown() {
        if (session != null) session.disconnect();
    }

    private String readOutput(String command, ChannelExec channel, InputStream in) {
        StringBuffer sb = new StringBuffer();
        try {
            byte[] tmp = new byte[1024];
            while (true) {
                while (in.available() > 0) {
                    int i = in.read(tmp, 0, 1024);
                    if (i < 0) break;
                    sb.append(new String(tmp, 0, i));
                }
                if (channel.isClosed()) {
                    if (in.available() > 0) continue;
                    log.debug("exit-status: " + channel.getExitStatus());
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (Exception ee) {
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("failed to exec " + command, e);
        }
        return sb.toString();
    }

    private void connect(ChannelExec channel) {
        try {
            channel.connect();
        } catch (JSchException e) {
            throw new RuntimeException("failed to connect", e);
        }
    }

    private InputStream getInputStream(ChannelExec channel) {
        InputStream in = null;
        try {
            in = channel.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException("failed to get input stream on channel", e);
        }
        return in;
    }

    private ChannelExec openChannel(String command) {
        final ChannelExec channel;
        try {
            channel = (ChannelExec) session.openChannel("exec");
        } catch (JSchException e) {
            throw new RuntimeException("failed to open channel", e);
        }
        channel.setCommand(command);
        // X Forwarding
        // channel.setXForwarding(true);
        channel.setInputStream(null);
        channel.setOutputStream(System.out);
        channel.setErrStream(System.err);
        return channel;
    }

    public static void main(String[] args) {
        RemoteConnection rc = new RemoteConnection("n1");
        try {
            System.out.println("sudo ls: " + rc.exec("sudo ls"));
            System.out.println("ls -la:" + rc.exec("ls -l"));
        } finally {
            rc.shutdown();
        }
    }

    @Override
    public String toString() {
        return "RemoteConnection{" +
                "host='" + host + '\'' +
                '}';
    }
}
