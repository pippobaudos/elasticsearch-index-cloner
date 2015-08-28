package com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util;

import com.roncia.elasticsearch.index.IndexRef;
import org.apache.commons.cli.*;

/**
 *
 * CommandLine static helper functions to manipulate command line arguments
 *
 * Created on 28/08/15.
 * @author moses.mansaray
 */
public class CommandLineUtil {

    public static CommandLine readCommandLine(String[] args) throws ParseException {
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

    private static Options createOptions() {
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
        options.addOption(Option.builder("dstIndexReplicas").hasArg().desc("destination: index number of replicas").build());
        options.addOption("keepDstIndex", true, "delete destination index if already existing");
        return options;
    }

    private static void help(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
        System.exit(0);
    }

    public static String[] buildCloneArguments(IndexRef src, IndexRef dst, String dstIndexReplicas) {
        return new String[]{
                "-srcHost", src.getHost(), "-srcUser", src.getUser(), "-srcPwd", src.getPwd(), "-srcIndex", src.getIndexName(),
                "-dstHost", dst.getHost(), "-dstUser", dst.getUser(), "-dstPwd", dst.getPwd(), "-dstIndex", dst.getIndexName(),
                "-dstIndexReplicas", dstIndexReplicas
        };
    }
}
