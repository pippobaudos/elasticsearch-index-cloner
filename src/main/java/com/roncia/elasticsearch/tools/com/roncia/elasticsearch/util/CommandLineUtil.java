package com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util;

import com.roncia.elasticsearch.index.IndexRef;
import org.apache.commons.cli.*;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * CommandLine static helper functions to manipulate command line arguments.
 *
 * Created on 28/08/15.
 * @author moses.mansaray
 */
public class CommandLineUtil {

    private final static Logger LOGGER = Logger.getLogger(CommandLineUtil.class.getName());

    /**
     * @param src the index reference of source
     * @param dst the index reference of destination
     * @param dstIndexReplicas the number if replicas to set for the new cloned index
     * @return  the string array of arguments
     */
    public static String[] buildCloneArguments(IndexRef src, IndexRef dst, String dstIndexReplicas) {
        return new String[]{
            "-srcHost", src.getHost(), "-srcUser", src.getUser(), "-srcPwd", src.getPwd(), "-srcIndex", src.getIndexName(),
            "-dstHost", dst.getHost(), "-dstUser", dst.getUser(), "-dstPwd", dst.getPwd(), "-dstIndex", dst.getIndexName(),
            "-dstIndexReplicas", dstIndexReplicas
        };
    }

    /**
     * Builds a commandLine key pair values from an array of strings
     *
     * @param args the command line arguments
     * @return the list of atomic option and value tokens
     * @throws ParseException if unable to parse any of the given arguments against the available options
     */
    public static CommandLine readCommandLine(String[] args) throws ParseException {
        CommandLine cmd = null;
        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();
        try {
          cmd = parser.parse(options, args);
        }
        catch (ParseException e) {
            LOGGER.log(Level.SEVERE, "Unable to parse given arguments against the available options : " + e.getMessage());
            help(options);
        }
        return cmd;
    }

    protected static Options createOptions() {
        Options options = new Options();
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
        System.exit(0);//TODO: review: maybe we can throw an uncaught exceptions which can bubble up instead of a kill!
    }

}
