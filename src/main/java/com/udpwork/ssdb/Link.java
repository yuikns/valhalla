package com.udpwork.ssdb;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Link {
    private Socket sock = null;
    private MemoryStream input = new MemoryStream();
    private String host;
    private int port;
    private int timeout_ms;

    public Link(String host, int port) throws Exception {
        this(host, port, 0);
    }

    public Link(String host, int port, int timeout_ms) throws Exception {
        this.host = host;
        this.port = port;
        this.timeout_ms = timeout_ms;
        init(host, port, timeout_ms);
    }

    private void init(String host, int port, int timeout_ms) throws Exception {
        sock = new Socket(host, port);
        if (timeout_ms > 0) {
            sock.setSoTimeout(timeout_ms);
        }
        sock.setTcpNoDelay(true);
    }

    public void relink() throws Exception {
        close();
        init(this.host, this.port, this.timeout_ms);
        input = new MemoryStream();
    }

    public void close() {
        try {
            if (sock != null)
                sock.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the closed state of the socket.
     *
     * @return true if the socket has been closed
     * @since 1.4
     */
    public boolean isClosed() {
        return sock == null || sock.isClosed();
    }

    /**
     * Returns the connection state of the socket.
     * <p>
     * Note: Closing a socket doesn't clear its connection state, which means
     * this method will return {@code true} for a closed socket
     * (see {@link #isClosed()}) if it was successfuly connected prior
     * to being closed.
     *
     * @return true if the socket was successfuly connected to a server
     * @since 1.4
     */
    public boolean isActive() {
        return sock != null && sock.isConnected();
    }

    /**
     * Returns the connection state of the socket.
     * <p>
     * Note: Closing a socket doesn't clear its connection state, which means
     * this method will return {@code true} for a closed socket
     * (see {@link #isClosed()}) if it was successfuly connected prior
     * to being closed.
     *
     * @return true if the socket was successfuly connected to a server
     * @since 1.4
     */
    public boolean isConnected() {
        return sock != null && sock.isConnected();
    }

    public Response request(String cmd, byte[]... params) throws Exception {
        ArrayList<byte[]> list;
        list = new ArrayList<>();
        Collections.addAll(list, params);
        return this.request(cmd, list);
    }

    public Response request(String cmd, String... params) throws Exception {
        ArrayList<byte[]> list;
        list = new ArrayList<>();
        for (String s : params) {
            list.add(s.getBytes());
        }
        return this.request(cmd, list);
    }

    public Response request(String cmd, List<byte[]> params) throws Exception {
        MemoryStream buf = new MemoryStream(4096);
        Integer len = cmd.length();
        buf.write(len.toString());
        buf.write('\n');
        buf.write(cmd);
        buf.write('\n');
        for (byte[] bs : params) {
            len = bs.length;
            buf.write(len.toString());
            buf.write('\n');
            buf.write(bs);
            buf.write('\n');
        }
        buf.write('\n');
        send(buf);

        List<byte[]> list = recv();
        return new Response(list);
    }

    private void send(MemoryStream buf) throws Exception {
        //System.out.println(">> " + buf.printable());
        OutputStream os = sock.getOutputStream();
        os.write(buf.buf, buf.data, buf.size);
        os.flush();
    }

    private List<byte[]> recv() throws Exception {
        input.nice();
        InputStream is = sock.getInputStream();
        while (true) {
            List<byte[]> ret = parse();
            if (ret != null) {
                return ret;
            }
            byte[] bs = new byte[8192];
            int len = is.read(bs);
            //System.out.println("<< " + (new MemoryStream(bs, 0, len)).printable());
            input.write(bs, 0, len);
        }
    }

    private List<byte[]> parse() {
        ArrayList<byte[]> list;
        list = new ArrayList<>();
        byte[] buf = input.buf;

        int idx = 0;
        while (true) {
            int pos = input.memchr('\n', idx);
            //System.out.println("pos: " + pos + " idx: " + idx);
            if (pos == -1) {
                break;
            }
            if (pos == idx || (pos == idx + 1 && buf[idx] == '\r')) {
                // ignore empty leading lines
                if (list.isEmpty()) {
                    idx += 1; // if '\r', next time will skip '\n'
                    continue;
                } else {
                    input.decr(idx + 1);
                    return list;
                }
            }
            String str = new String(buf, input.data + idx, pos - idx);
            int len = Integer.parseInt(str);
            idx = pos + 1;
            if (idx + len >= input.size) {
                break;
            }
            byte[] data = Arrays.copyOfRange(buf, input.data + idx, input.data + idx + len);
            //System.out.println("len: " + len + " data: " + data.length);
            idx += len + 1; // skip '\n'
            list.add(data);
        }
        return null;
    }

}

