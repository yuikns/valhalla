package com.udpwork.ssdb;

import java.util.List;

/**
 * SSDB Java client SDK. Example:
 * <p>
 * <p>
 * <pre>
 * SSDB com.udpwork.ssdb = new SSDB(&quot;127.0.0.1&quot;, 8888);
 * com.udpwork.ssdb.set(&quot;a&quot;, &quot;123&quot;);
 * byte[] val = com.udpwork.ssdb.get(&quot;a&quot;);
 * com.udpwork.ssdb.close();
 * </pre>
 */
public class SSDB {
    /**
     *
     */
    protected Link link;
    private boolean invalid = false;

    public SSDB(String host, int port) throws Exception {
        this(host, port, 0);
    }

    public SSDB(String host, int port, int timeout_ms) throws Exception {
        link = new Link(host, port, timeout_ms);
    }

    public void relink() throws Exception {
        link.relink();
        invalid = false;
    }

    public void close() {
        link.close();
    }

    public boolean isActive() {
        return !invalid && !link.isClosed() && link.isConnected();
    }

    public Response request(String cmd, byte[]... params) throws Exception {
        try {
            return link.request(cmd, params); // request , will return
        } catch (Exception e) {
            e.printStackTrace();
            this.relink(); //
            throw e;
        }
    }

    public Response request(String cmd, String... params) throws Exception {
        return link.request(cmd, params);
    }

    public Response request(String cmd, List<byte[]> params) throws Exception {
        return link.request(cmd, params);
    }

    public Boolean ping() {
        Response resp;
        try {
            resp = link.request("ping", "");
            if (resp.ok()) {
                return true;
            }
            resp.exception();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

	/* kv */

    public void expire(byte[] key, long ttl) throws Exception {
        expire(new String(key), ttl);
    }

    public void expire(String key, long ttl) throws Exception {
        Response resp = link.request("expire", key, (new Long(ttl)).toString());
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }


    /**
     * @param key key
     * @return -1L if not exists
     * @throws Exception
     */
    public Long ttl(byte[] key) throws Exception {
        Response resp = link.request("ttl", key);
        if (resp.not_found()) {
            return -1L;
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        if (resp.ok()) {
            return Long.parseLong(new String(resp.raw.get(1)));
        }
        resp.exception();
        return 0L;
    }

    /**
     * @param key key
     * @return null if not found.
     * @throws Exception
     */
    public Long ttl(String key) throws Exception {
        return ttl(key.getBytes());
    }

    public void set(byte[] key, byte[] val) throws Exception {
        Response resp = link.request("set", key, val);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void set(String key, byte[] val) throws Exception {
        set(key.getBytes(), val);
    }

    public void set(String key, String val) throws Exception {
        set(key.getBytes(), val.getBytes());
    }

    public void setx(byte[] key, byte[] val, long ttl) throws Exception {
        Response resp = link.request("setx", key, val, (new Long(ttl)).toString().getBytes());
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void setx(String key, byte[] val, long ttl) throws Exception {
        setx(key.getBytes(), val, ttl);
    }

    public void setx(String key, String val, long ttl) throws Exception {
        setx(key.getBytes(), val.getBytes(), ttl);
    }

    public void setnx(byte[] key, byte[] val, long ttl) throws Exception {
        Response resp = link.request("setnx", key, val, (new Long(ttl)).toString().getBytes());
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void setnx(String key, byte[] val, long ttl) throws Exception {
        setnx(key.getBytes(), val, ttl);
    }

    public void setnx(String key, String val, long ttl) throws Exception {
        setnx(key, val.getBytes(), ttl);
    }


    public void del(byte[] key) throws Exception {
        Response resp = link.request("del", key);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void del(String key) throws Exception {
        del(key.getBytes());
    }

    /***
     * @param key
     * @return null if not found
     * @throws Exception
     */
    public byte[] get(byte[] key) throws Exception {
        Response resp = link.request("get", key);
        if (resp.not_found()) {
            return null;
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        if (resp.ok()) {
            return resp.raw.get(1);
        }
        resp.exception();
        return null;
    }

    /***
     * @param key
     * @return null if not found
     * @throws Exception
     */
    public byte[] get(String key) throws Exception {
        return get(key.getBytes());
    }

    private Response _scan(String cmd, String key_start, String key_end,
                           int limit) throws Exception {
        if (key_start == null) {
            key_start = "";
        }
        if (key_end == null) {
            key_end = "";
        }
        Response resp = link.request(cmd, key_start, key_end, (new Integer(
                limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        resp.buildMap();
        return resp;
    }

    public Response scan(String key_start, String key_end, int limit)
            throws Exception {
        return _scan("scan", key_start, key_end, limit);
    }

    public Response rscan(String key_start, String key_end, int limit)
            throws Exception {
        return _scan("rscan", key_start, key_end, limit);
    }

    private Response _zrange(String cmd, String name, int offset, int limit)
            throws Exception {
        Response resp = link.request(cmd, name,
                (new Integer(offset)).toString(),
                (new Integer(limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        int len = resp.raw.size() - 1;
        for (int i = 1; i < len; i += 2) {
            byte[] k = resp.raw.get(i);
            byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            resp.items.put(k, v);
        }
        return resp;
    }

    public Response zrange(String name, int offset, int limit) throws Exception {
        return _zrange("zrange", name, offset, limit);
    }

    public Response zrrange(String name, int offset, int limit)
            throws Exception {
        return _zrange("zrrange", name, offset, limit);
    }

    /**
     * return count of keys in [scoreStart, scoreEnd]
     *
     * @param name       name
     * @param scoreStart score start , null for -inf
     * @param scoreEnd   score end, null for +inf
     * @return count
     * @throws Exception key not found
     */
    public long zcount(String name, Long scoreStart, Long scoreEnd) throws Exception {
        Response resp = link.request(
                "zcount",
                name,
                (scoreStart == null ? "" : scoreStart.toString()),
                (scoreEnd == null ? "" : scoreEnd.toString()));
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        return Long.parseLong(new String(resp.raw.get(1)));
    }

    private Response _zlist(String cmd, String key_start, String key_end,
                            int limit) throws Exception {
        if (key_start == null) {
            key_start = "";
        }
        if (key_end == null) {
            key_end = "";
        }
        Response resp = link.request(cmd, key_start, key_end, (new Integer(
                limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        for (int i = 1; i < resp.raw.size(); i++) {
            byte[] k = resp.raw.get(i);
            resp.keys.add(k);
        }
        return resp;
    }

    public Response zlist(String key_start, String key_end, int limit)
            throws Exception {
        return _zlist("zlist", key_start, key_end, limit);
    }

    public Response zrlist(String key_start, String key_end, int limit)
            throws Exception {
        return _zlist("zrlist", key_start, key_end, limit);
    }

    public long incr(String key, long by) throws Exception {
        Response resp = link.request("incr", key, (new Long(by)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        long ret = 0;
        ret = Long.parseLong(new String(resp.raw.get(1)));
        return ret;
    }

	/* hashmap */

    public void hset(String name, byte[] key, byte[] val) throws Exception {
        Response resp = link.request("hset", name.getBytes(), key, val);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void hset(String name, String key, byte[] val) throws Exception {
        this.hset(name, key.getBytes(), val);
    }

    public void hset(String name, String key, String val) throws Exception {
        this.hset(name, key, val.getBytes());
    }

    public void hdel(String name, byte[] key) throws Exception {
        Response resp = link.request("hdel", name.getBytes(), key);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void hdel(String name, String key) throws Exception {
        this.hdel(name, key.getBytes());
    }

    /**
     * @param name
     * @param key
     * @return null if not found
     * @throws Exception
     */
    public byte[] hget(String name, byte[] key) throws Exception {
        Response resp = link.request("hget", name.getBytes(), key);
        if (resp.not_found()) {
            return null;
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        if (resp.ok()) {
            return resp.raw.get(1);
        }
        resp.exception();
        return null;
    }

    /**
     * @param name
     * @param key
     * @return null if not found
     * @throws Exception
     */
    public byte[] hget(String name, String key) throws Exception {
        return hget(name, key.getBytes());
    }

    private Response _hscan(String cmd, String name, String key_start,
                            String key_end, int limit) throws Exception {
        if (key_start == null) {
            key_start = "";
        }
        if (key_end == null) {
            key_end = "";
        }
        Response resp = link.request(cmd, name, key_start, key_end,
                (new Integer(limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        int len = resp.raw.size() - 1;
        for (int i = 1; i < len; i += 2) {
            byte[] k = resp.raw.get(i);
            byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            resp.items.put(k, v);
        }
        return resp;
    }

    public Response hscan(String name, String key_start, String key_end,
                          int limit) throws Exception {
        return this._hscan("hscan", name, key_start, key_end, limit);
    }

    public Response hrscan(String name, String key_start, String key_end,
                           int limit) throws Exception {
        return this._hscan("hrscan", name, key_start, key_end, limit);
    }

    public long hincr(String name, String key, long by) throws Exception {
        Response resp = link.request("hincr", name, key,
                (new Long(by)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        long ret = 0;
        ret = Long.parseLong(new String(resp.raw.get(1)));
        return ret;
    }

    public void hclear(byte[] name) throws Exception {
        Response resp = link.request("hclear", name);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void hclear(String name) throws Exception {
        hclear(name.getBytes());
    }

    /* zset */
    public long zsize(String name) throws Exception {
        Response resp = link.request("zsize", name);
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        return Long.parseLong(new String(resp.raw.get(1)));
    }

    public void zset(String name, byte[] key, long score) throws Exception {
        Response resp = link.request("zset", name.getBytes(), key, (new Long(
                score)).toString().getBytes());
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void zset(String name, String key, long score) throws Exception {
        zset(name, key.getBytes(), score);
    }

    public void zdel(String name, byte[] key) throws Exception {
        Response resp = link.request("zdel", name.getBytes(), key);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void zdel(String name, String key) throws Exception {
        this.zdel(name, key.getBytes());
    }

    public void zrem(String name, byte[] key) throws Exception {
        this.zdel(name, key);
    }

    public void zrem(String name, String key) throws Exception {
        this.zdel(name, key.getBytes());
    }

    /**
     * @param name name
     * @param key  key
     * @return null if not found.
     * @throws Exception
     */
    public Long zget(String name, byte[] key) throws Exception {
        Response resp = link.request("zget", name.getBytes(), key);
        if (resp.not_found()) {
            return null;
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        if (resp.ok()) {
            return Long.parseLong(new String(resp.raw.get(1)));
        }
        resp.exception();
        return 0L;
    }

    /**
     * @param name name
     * @param key  key
     * @return null if not found.
     * @throws Exception
     */
    public Long zget(String name, String key) throws Exception {
        return zget(name, key.getBytes());
    }

    private Response _zscan(String cmd, String name, String key,
                            Long score_start, Long score_end, int limit) throws Exception {
        if (key == null) {
            key = "";
        }
        String ss = "";
        if (score_start != null) {
            ss = score_start.toString();
        }
        String se = "";
        if (score_end != null) {
            se = score_end.toString();
        }
        Response resp = link.request(cmd, name, key, ss, se,
                (new Integer(limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        int len = resp.raw.size() - 1;
        for (int i = 1; i < len; i += 2) {
            byte[] k = resp.raw.get(i);
            byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            resp.items.put(k, v);
        }
        return resp;
    }

    public Response zscan(String name, String key, Long score_start,
                          Long score_end, int limit) throws Exception {
        return this._zscan("zscan", name, key, score_start, score_end, limit);
    }

    public Response zrscan(String name, String key, Long score_start,
                           Long score_end, int limit) throws Exception {
        return this._zscan("zrscan", name, key, score_start, score_end, limit);
    }

    public long zincr(String name, String key, long by) throws Exception {
        Response resp = link.request("zincr", name, key,
                (new Long(by)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        long ret = 0;
        ret = Long.parseLong(new String(resp.raw.get(1)));
        return ret;
    }

    public void zclear(byte[] name) throws Exception {
        Response resp = link.request("zclear", name);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void zclear(String name) throws Exception {
        zclear(name.getBytes());
    }

    /* queue */
    public long qsize(String name) throws Exception {
        Response resp = link.request("qsize", name);
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        long ret = Long.parseLong(new String(resp.raw.get(1)));
        return ret;
    }

    public String qfront(String name) throws Exception {
        Response resp = link.request("qfront", name);
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        return new String(resp.raw.get(1));
    }

    public String qback(String name) throws Exception {
        Response resp = link.request("qback", name);
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        return new String(resp.raw.get(1));
    }

    public String qget(String name, long index) throws Exception {
        Response resp = link
                .request("qget", name, (new Long(index)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() != 2) {
            throw new Exception("Invalid response");
        }
        return new String(resp.raw.get(1));
    }

    public void qset(String name, long index, String data) throws Exception {
        Response resp = link.request("qset", name,
                (new Long(index)).toString(), data);
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public void qset(String name, long index, byte[] data) throws Exception {
        qset(name, index, new String(data));
    }

    public void qpush_back(String name, String data) throws Exception {
        Response resp = link.request("qpush_back", name, data);
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public void qpush_back(String name, byte[] data) throws Exception {
        qpush_back(name, new String(data));
    }

    public void qpush(String name, String data) throws Exception {
        qpush_back(name, data);
    }

    public void qpush(String name, byte[] data) throws Exception {
        qpush_back(name, new String(data));
    }

    public void qpush_front(String name, String data) throws Exception {
        Response resp = link.request("qpush_front", name, data);
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public void qpush_front(String name, byte[] data) throws Exception {
        qpush_front(name, new String(data));
    }

    public Response qpop_back(String name, int size) throws Exception {
        Response resp = link.request("qpop_back", name,
                (new Integer(size)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() < 2) {
            throw new Exception("Invalid response");
        }
        for (int i = 1; i < resp.raw.size(); i++) {
            byte[] k = resp.raw.get(i);
            // byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            // resp.items.put(Integer.toString(i - 1).getBytes(), k);
        }
        return resp;
    }

    public Response qpop_front(String name, int size) throws Exception {
        Response resp = link.request("qpop_front", name,
                (new Integer(size)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        if (resp.raw.size() < 2) {
            throw new Exception("Invalid response");
        }
        for (int i = 1; i < resp.raw.size(); i++) {
            byte[] k = resp.raw.get(i);
            // byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            // resp.items.put(Integer.toString(i - 1).getBytes(), k);
        }
        return resp;
    }

    /**
     * @param name
     * @param size
     * @return
     * @throws Exception
     */
    public Response qpop(String name, int size) throws Exception {
        return qpop_front(name, size);
    }

    public void qtrim_back(String name, int size) throws Exception {
        Response resp = link.request("qtrim_back", name,
                (new Integer(size)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public void qtrim_front(String name, int size) throws Exception {
        Response resp = link.request("qtrim_front", name,
                (new Integer(size)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
    }

    /**
     * @param cmd
     * @param key_start
     * @param key_end
     * @param limit
     * @return
     * @throws Exception
     */
    private Response _qlist(String cmd, String key_start, String key_end,
                            int limit) throws Exception {
        Response resp = link.request(cmd, key_start, key_end, (new Integer(
                limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        for (int i = 1; i < resp.raw.size(); i++) {
            byte[] k = resp.raw.get(i);
            // byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            // resp.items.put(Integer.toString(i - 1).getBytes(), k);
        }
        return resp;
    }

    /**
     * @param key_start
     * @param key_end
     * @param limit
     * @return
     * @throws Exception
     */
    public Response qlist(String key_start, String key_end, int limit)
            throws Exception {
        return this._qlist("qlist", key_start, key_end, limit);
    }

    /**
     * @param key_start
     * @param key_end
     * @param limit
     * @return
     * @throws Exception
     */
    public Response qrlist(String key_start, String key_end, int limit)
            throws Exception {
        return this._qlist("qrlist", key_start, key_end, limit);
    }

    /**
     * @param cmd
     * @param name
     * @param offset
     * @param limit
     * @return
     * @throws Exception
     */
    private Response _qrange(String cmd, String name, int offset, int limit)
            throws Exception {
        Response resp = link.request(cmd, name,
                (new Integer(offset)).toString(),
                (new Integer(limit)).toString());
        if (!resp.ok()) {
            resp.exception();
        }
        for (int i = 1; i < resp.raw.size(); i++) {
            byte[] k = resp.raw.get(i);
            // byte[] v = resp.raw.get(i + 1);
            resp.keys.add(k);
            // resp.items.put(Integer.toString(i - 1).getBytes(), k);
        }
        return resp;
    }

    public Response qrange(String name, int offset, int limit) throws Exception {
        return this._qrange("qrange", name, offset, limit);
    }

    public Response qslice(String name, int offset, int limit) throws Exception {
        return this._qrange("qslice", name, offset, limit);
    }

    /**
     * @param name
     * @throws Exception
     */
    public void qclear(byte[] name) throws Exception {
        Response resp = link.request("qclear", name);
        if (resp.ok()) {
            return;
        }
        resp.exception();
    }

    public void qclear(String name) throws Exception {
        qclear(name.getBytes());
    }

    /****************/

    public Response multi_get(String... keys) throws Exception {
        Response resp = link.request("multi_get", keys);
        if (!resp.ok()) {
            resp.exception();
        }
        resp.buildMap();
        return resp;
    }

    public Response multi_get(byte[]... keys) throws Exception {
        Response resp = link.request("multi_get", keys);
        if (!resp.ok()) {
            resp.exception();
        }
        resp.buildMap();
        return resp;
    }

    public void multi_set(String... kvs) throws Exception {
        if (kvs.length % 2 != 0) {
            throw new Exception("Invalid arguments count");
        }
        Response resp = link.request("multi_set", kvs);
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public void multi_set(byte[]... kvs) throws Exception {
        if (kvs.length % 2 != 0) {
            throw new Exception("Invalid arguments count");
        }
        Response resp = link.request("multi_set", kvs);
        if (!resp.ok()) {
            resp.exception();
        }
    }

    public Response multi_del(String... keys) throws Exception {
        Response resp = link.request("multi_del", keys);
        if (!resp.ok()) {
            resp.exception();
        }
        resp.buildMap();
        return resp;
    }

    public Response multi_del(byte[]... keys) throws Exception {
        Response resp = link.request("multi_del", keys);
        if (!resp.ok()) {
            resp.exception();
        }
        resp.buildMap();
        return resp;
    }

    public void labelAsInvalid() {
        this.invalid = true;
    }
}
