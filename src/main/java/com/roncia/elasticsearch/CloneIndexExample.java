package com.roncia.elasticsearch;

import com.roncia.elasticsearch.application.IndexCloner;
import com.roncia.elasticsearch.index.IndexRef;
import com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util.CommandLineUtil;
import org.apache.commons.cli.ParseException;

import java.io.IOException;

/**
 * Example configuration for Clone Index execution
 *
 * Created on 28/08/2015
 * @author moses.mansaray
 */
public class CloneIndexExample {

    public static void main(String[] args) throws ParseException, InterruptedException, IOException {

        String srcIndexName = "test_v6";
        String dstIndexName = "test_v6";

        IndexRef src = new IndexRef("stg-server", "username", "password", srcIndexName);
        IndexRef dstQa   = new IndexRef("dev-server", "username", "password", dstIndexName);

        final String[] cloneArguments = CommandLineUtil.buildCloneArguments(src, dstQa, "0");
        IndexCloner.main(cloneArguments);
    }
}
