package com.roncia.elasticsearch.tools;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.cluster.Health;
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
import java.util.Map.Entry;


public class IndexClone {


   /**
   * @param args
   * @throws IOException 
   */
  public static void main(String[] args) throws IOException, ParseException, InterruptedException {
      CommandLine cmd = readCommandLine(args);

      String srcHost = cmd.getOptionValue("srcHost");
      String srcIndex = cmd.getOptionValue("srcIndex");
      String srcUser = cmd.getOptionValue("srcUser");
      String srcPwd = cmd.getOptionValue("srcPwd");

      String dstHost = cmd.getOptionValue("dstHost");
      String dstIndex = cmd.getOptionValue("dstIndex");
      String dstUser = cmd.getOptionValue("dstUser");
      String dstPwd = cmd.getOptionValue("dstPwd");

      boolean keepDstIndex = cmd.hasOption("keepDstIndex");

      JestClient src = getAuthenticatedClient("http://" + srcHost, srcUser, srcPwd);
      JestClient dst = getAuthenticatedClient("http://" + dstHost, dstUser, dstPwd);

      if (!keepDstIndex) {
        deleteIfExistAndLoadSettingsMappings(src, dst, srcIndex, dstIndex);
        waitNotInRedState(dst);
      }
      cloneData(src, dst, srcIndex, dstIndex);
  }

    private static CommandLine readCommandLine(String[] args) throws ParseException {
        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();
        CommandLine cmd;
        try {
          cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            help(options);
          throw e;
        }
        return cmd;
    }

    private static void waitNotInRedState(JestClient dst) throws IOException, InterruptedException {
        String clusterStatus = null;
        do {
          JestResult result = dst.execute(new Health.Builder().build());
            clusterStatus = result.getJsonObject().get("status").getAsString();
           Thread.sleep(500);
        } while ("red".equals(clusterStatus));
    }

    private static Options createOptions() throws ParseException {
        Options options = new Options();
        options.addOption(Option.builder("srcHost").hasArg().desc("source: host:port (e.g. localhost:9200)").required().build());
        options.addOption(Option.builder("srcHost").hasArg().desc("source: host:port (e.g. localhost:9200)").required().build());
        options.addOption(Option.builder("srcIndex").hasArg().desc("source: index name").required().build());
        options.addOption(Option.builder("srcUser").hasArg().desc("source: user authentication").build());
        options.addOption(Option.builder("srcPwd").hasArg().desc("source: password authentication").build());
        options.addOption(Option.builder("dstHost").hasArg().desc("destination: host:port (e.g. localhost:9200)").required().build());
        options.addOption(Option.builder("dstIndex").hasArg().desc("destination: index name").required().build());
        options.addOption(Option.builder("dstUser").hasArg().desc("destination: user authentication").build());
        options.addOption(Option.builder("dstPwd").hasArg().desc("destination: password authentication").build());
        options.addOption("keepDstIndex",   true, "delete destination index if already existing");
        return options;
    }

    public static JestClient getAuthenticatedClient(String host, String user, String pwd) {
        JestClientFactory factory = new JestClientFactory();
        HttpClientConfig.Builder builder = new HttpClientConfig.Builder(host);
        if (user != null || pwd != null) {
            builder = builder.defaultCredentials(user, pwd);
        }
        factory.setHttpClientConfig(builder.build());
        return factory.getObject();
    }

    private static void cloneData(JestClient src, JestClient dst, String indexSrc, String indexDst) throws IOException {
        int from = 0;
        int sizePage = 100;
        int nHits = 0;
        int totHits = 0;

        String scrollId = null;
        JestResult ret = null;
        while(true) {

          // Only on first page: Query
          if (scrollId == null)  {
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
          JestResult response = dst.execute(bulk.build());
          System.out.println(response.getJsonString());
        }

        System.out.println("Copied succesfully " + totHits + " documents");
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

    private static void deleteIfExistAndLoadSettingsMappings(JestClient src, JestClient dst, String indexSrc, String indexDst) throws IOException {
        GetSettings getSettings = new GetSettings.Builder().addIndex(indexSrc).prefixQuery(indexSrc).build();
        JestResult result = src.execute(getSettings);
        JsonElement srcLoad = result.getJsonObject().get(indexSrc);
        if (srcLoad == null) {
            throw new RuntimeException("The source index " + indexSrc + " doesn't exist. Impossible to continue!") ;
        }

        String currentSettings = srcLoad.getAsJsonObject().get("settings").getAsJsonObject().get("index").getAsJsonObject().toString();

        DeleteIndex indicesExists = new DeleteIndex.Builder(indexDst).build();
        JestResult delete = dst.execute(indicesExists);
        System.out.println("delete: " + delete.getJsonString());
        JestResult create = dst.execute(new CreateIndex.Builder(indexDst).settings(currentSettings).build());
        System.out.println("create: " + create.getJsonString());

        GetMapping getMapping = new GetMapping.Builder().addIndex(indexSrc).build();
        JsonElement oldMapping = src.execute(getMapping).getJsonObject().get(indexSrc).getAsJsonObject().get("mappings");
        if (oldMapping instanceof JsonObject) {
          JsonObject m = (JsonObject)oldMapping;
          for (Entry<String, JsonElement> e : m.entrySet()) {
            String type = e.getKey();
            String typeMapping = oldMapping.getAsJsonObject().get(type).getAsJsonObject().toString();
            PutMapping putMapping = new PutMapping.Builder(indexDst, type, typeMapping).build();
            dst.execute(putMapping);
          }
        }
        System.out.println("oldMapping: " + oldMapping.toString());

    }


    private static void help(Options options) {
        HelpFormatter formater = new HelpFormatter();
        formater.printHelp("Main", options);
        System.exit(0);
    }
}
