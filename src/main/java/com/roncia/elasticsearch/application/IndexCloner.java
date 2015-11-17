package com.roncia.elasticsearch.application;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util.CommandLineUtil;
import com.sun.jmx.snmp.Timestamp;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import io.searchbox.indices.settings.GetSettings;
import io.searchbox.params.Parameters;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;


public class IndexCloner {

    private static final Logger LOGGER = Logger.getLogger(IndexCloner.class.getName());

    /**
     * Index Cloner application main function
     *
     * @param args command line arguments representing source and destination index properties
     * @throws IOException
     * @throws ParseException
     * @throws InterruptedException
     * @throws RuntimeException
     */
    public static void main(String args[]) throws IOException, ParseException, InterruptedException, RuntimeException {
        CommandLine cmd = getCommandLine(args);
        String srcIndex = cmd.getOptionValue("srcIndex");
        String dstIndex = cmd.getOptionValue("dstIndex");
        JestClient src = getClient("srcHost", "srcUser", "srcPwd", cmd);
        JestClient dst = getClient("dstHost", "dstUser", "dstPwd", cmd);
        createDestinationIndexFromSource(srcIndex, dstIndex, src, dst, cmd);
        cloneData(src, dst, srcIndex, dstIndex);
    }

    private static CommandLine getCommandLine(String[] args) throws ParseException {
        return CommandLineUtil.readCommandLine(args);
    }

    private static void createDestinationIndexFromSource(
            String srcIndex, String dstIndex, JestClient src, JestClient dst, CommandLine cmd)
            throws IOException, InterruptedException {
        boolean keepDstIndex = Boolean.parseBoolean(cmd.getOptionValue("keepDstIndex"));
        if (!keepDstIndex) {
            copySettings(srcIndex, dstIndex, src, dst, cmd);
        } else {
            logInformation("Skip : Copying settings");
        }
    }

    private static void copySettings(String srcIndex, String dstIndex, JestClient src, JestClient dst, CommandLine cmd) throws IOException, InterruptedException {
        logInformation("Copying settings");
        JsonElement srcLoad = getSourceIndexSettings(src, srcIndex);
        modifyIndexReplicaConfigurations(cmd, srcLoad);
        deleteDestinationIndex(dst, dstIndex);
        createDestinationIndexFromSourceSettings(dst, dstIndex, srcLoad);
        applySourceMappingToDestinationIndex(src, dst, srcIndex, dstIndex);
        waitWhilstDestinationIndexIsInRedState(dst);
    }

    private static String getCurrentSettings(JsonElement srcLoad) {
        return srcLoad.getAsJsonObject().get("settings").getAsJsonObject().get("index").getAsJsonObject().toString();
    }

    private static JestClient getClient(String host, String user, String pwd, CommandLine cmd) {
        return getAuthenticatedClient(
                "http://" + cmd.getOptionValue(host), cmd.getOptionValue(user), cmd.getOptionValue(pwd));
    }

    private static void waitWhilstDestinationIndexIsInRedState(JestClient dst)
            throws IOException, InterruptedException {
        String clusterStatus;
        do {
            JestResult result = dst.execute(new Health.Builder().build());
            clusterStatus = result.getJsonObject().get("status").getAsString();
            Thread.sleep(500);
        } while ("red".equals(clusterStatus));
    }

    private static JestClient getAuthenticatedClient(String host, String user, String pwd) {
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder(host);
        if (user != null && pwd != null) {
            builder = builder.defaultCredentials(user, pwd);
        }
        //substantially high timeout to give the application a chance to response respond with adequate msg if any
        factory.setHttpClientConfig(builder.connTimeout(3 * 60 * 1000).readTimeout(3 * 60 * 1000).build());
        return factory.getObject();
    }

    private static Builder bulkRequestBuilder(String indexDst, JsonArray hits) {
        Builder bulk = new Builder().defaultIndex(indexDst);
        for (JsonElement hit : hits) {
            JsonObject h = hit.getAsJsonObject();
            String id = h.get("_id").getAsString();
            String t = h.get("_type").getAsString();
            String source = h.get("_source").getAsJsonObject().toString();
            Index index = new Index.Builder(source).index(indexDst).type(t).id(id).build();
            bulk.addAction(index);
        }
        return bulk;
    }

    private static JsonElement getSourceIndexSettings(JestClient src, String indexSrc) throws IOException {
        GetSettings getSettings = new GetSettings.Builder().addIndex(indexSrc).prefixQuery(indexSrc).build();
        JestResult result = src.execute(getSettings);
        JsonElement srcLoad = result.getJsonObject().get(indexSrc);
        if (srcLoad == null) {
            throw new RuntimeException("The source index " + indexSrc + " doesn't exist. Impossible to continue!");
        }
        return srcLoad;
    }

    private static void modifyIndexReplicaConfigurations(CommandLine cmd, JsonElement srcLoad) {
        final JsonObject settings = srcLoad.getAsJsonObject().get("settings").getAsJsonObject();
        final JsonObject index = settings.get("index").getAsJsonObject();

        String dstIndexReplicas = cmd.getOptionValue("dstIndexReplicas");
        if (dstIndexReplicas != null && dstIndexReplicas.trim().length() > 0) {
            try {
                final int numberDstIndexReplicas = Integer.valueOf(dstIndexReplicas);
                index.addProperty("number_of_replicas", String.valueOf(numberDstIndexReplicas));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid Number for Replicas argument! " + e.getMessage());
            }
        }

        String dstIndexShards = cmd.getOptionValue("dstIndexShards");
        if (dstIndexShards != null && !dstIndexShards.isEmpty()) {
            try {
                final int numberDstIndexShards = Integer.valueOf(dstIndexShards);
                index.addProperty("number_of_shards", String.valueOf(numberDstIndexShards));
            } catch (NumberFormatException e) {
                throw new RuntimeException("Invalid Number for Replicas argument! " + e.getMessage());
            }
        }
    }

    private static void deleteDestinationIndex(JestClient dst, String indexDst) throws IOException {
        DeleteIndex indicesExists = new DeleteIndex.Builder(indexDst).build();
        JestResult delete = dst.execute(indicesExists);
        logInformation("delete: " + delete.getJsonString());
    }

    private static void createDestinationIndexFromSourceSettings(
            JestClient dst, String indexDst, JsonElement currentSettings)
            throws IOException {
        JestResult create = dst.execute(new CreateIndex.Builder(indexDst).settings(getCurrentSettings(currentSettings)).build());
        logInformation("create: " + create.getJsonString());
        confirmResponse(create, "Index not created properly!");
    }

    private static void applySourceMappingToDestinationIndex(
            JestClient src, JestClient dst, String indexSrc, String indexDst)
            throws RuntimeException, IOException {
        GetMapping getMapping = new GetMapping.Builder().addIndex(indexSrc).build();
        JsonElement oldMapping = src.execute(getMapping).getJsonObject().get(indexSrc).getAsJsonObject().get("mappings");
        if (oldMapping instanceof JsonObject) {
            JsonObject m = (JsonObject) oldMapping;
            for (Entry<String, JsonElement> e : m.entrySet()) {
                String type = e.getKey();
                String typeMapping = oldMapping.getAsJsonObject().get(type).getAsJsonObject().toString();
                PutMapping putMapping = new PutMapping.Builder(indexDst, type, typeMapping).build();

                JestResult dstResult = dst.execute(putMapping);
                confirmResponse(dstResult, "Mapping not loaded properly!");
            }
        }
        logInformation("oldMapping: " + oldMapping.toString());
    }

    private static void confirmResponse(JestResult result, String message) {
        if (!result.getJsonObject().get("acknowledged").getAsBoolean()) {
            throw new RuntimeException(message);
        }
    }

    private static void cloneData(JestClient src, JestClient dst, String indexSrc, String indexDst) throws IOException {
        logInformation("cloning data phase started");

        int from = 0;
        int sizePage = 100;
        int nHits;
        int totHits = 0;

        String scrollId = null;
        JestResult ret;
        while (true) {

            // Only on first page: Query
            if (scrollId == null) {
                String query = "{\"size\": " + sizePage + ", \"from\": " + from + "}";
                Search search = new Search.Builder(query).addIndex(indexSrc).setParameter(Parameters.SIZE, sizePage)
                        .setParameter(Parameters.SCROLL, "5m").build();
                ret = src.execute(search);
                scrollId = ret.getJsonObject().get("_scroll_id").getAsString();
            }
            // Since second page: Scroll
            else {
                SearchScroll scroll = new SearchScroll.Builder(scrollId, "5m")
                        .setParameter(Parameters.SIZE, sizePage).build();

                ret = src.execute(scroll);
            }

            JsonArray hits = ret.getJsonObject().get("hits").getAsJsonObject().get("hits").getAsJsonArray();
            nHits = hits.size();
            if (nHits == 0) {
                break;
            }

            totHits += nHits;
            Builder bulk = bulkRequestBuilder(indexDst, hits);

            JestResult response = null;
            try {
                Bulk build = bulk.build();
                logInformation(new Timestamp(System.currentTimeMillis()).toString());
                response = dst.execute(build);
            } catch (SocketTimeoutException e) {
                logInformation(" ------ socket exp");
                logInformation(new Timestamp(System.currentTimeMillis()).toString());
                e.printStackTrace();
            }
            logResponse(response);
        }
        logInformation("Copied successfully " + totHits + " documents");
        logInformation("cloning data phase finished");
    }

    private static void logResponse(JestResult response) {
        if (response != null) {
            logInformation(response.getJsonString());
        }
    }

    private static void logInformation(String message) {
        LOGGER.log(Level.INFO, message);
    }

}
