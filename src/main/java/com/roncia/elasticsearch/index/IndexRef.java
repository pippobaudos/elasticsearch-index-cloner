package com.roncia.elasticsearch.index;

/**
 * Index properties pojo
 *
 * Created on 30/07/15.
 * @author niccolo.becchi
 */
public class IndexRef {
    private final String host;
    private final String user;
    private final String pwd;
    private final String indexName;

    public IndexRef(String host, String user, String pwd, String indexName) {
        this.host = host;
        this.user = user;
        this.pwd = pwd;
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getPwd() {
        return pwd;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

}