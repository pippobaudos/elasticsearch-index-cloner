package com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util;

import com.roncia.elasticsearch.index.IndexRef;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;

import static com.roncia.elasticsearch.tools.com.roncia.elasticsearch.util.CommandLineUtil.*;
import static org.junit.Assert.assertEquals;

/**
 * Created on 31/08/2015.
 * @author moses.mansaray
 */
public class CommandLineUtilTest {

  @Rule
  public final ExpectedSystemExit exit = ExpectedSystemExit.none();

  @Test
  public void readCommandLineShouldReturnOptionsAsExpected() throws Exception {
    IndexRef srcIndex = new IndexRef("mySrcHost", "user1", "pwd1", "myIndex");
    IndexRef dstIndex = new IndexRef("myDstHost", "user2", "pwd2", "myIndexCopy");
    String[] cloneArguments = buildCloneArguments(srcIndex, dstIndex, "7", "4");
    CommandLine commandLine = readCommandLine(cloneArguments);
    int optionsLength = commandLine.getOptions().length;

    assertEquals("Options size is not as expected", 9, optionsLength);
    assertEquals("Src Host not as expected", "mySrcHost", commandLine.getOptionValue("srcHost"));
    assertEquals("Src User not as expected", "user1", commandLine.getOptionValue("srcUser"));
    assertEquals("Src Pwd not as expected", "pwd1", commandLine.getOptionValue("srcPwd"));
    assertEquals("Src Index not as expected", "myIndex", commandLine.getOptionValue("srcIndex"));
    assertEquals("Dst Host not as expected", "myDstHost", commandLine.getOptionValue("dstHost"));
    assertEquals("Dst User not as expected", "user2", commandLine.getOptionValue("dstUser"));
    assertEquals("Dst Pwd not as expected", "pwd2", commandLine.getOptionValue("dstPwd"));
    assertEquals("Dst Index not as expected", "myIndexCopy", commandLine.getOptionValue("dstIndex"));
    assertEquals("Replicas not as expected", "7", commandLine.getOptionValue("dstIndexReplicas"));
    assertEquals("Replicas not as expected", "4", commandLine.getOptionValue("dstIndexShards"));

  }

  @Test
  public void buildCloneArgumentsShouldMatchNumberOfOptions() throws Exception {
    int optionsLength = createOptions().getOptions().size() - 1; // minus keepDstIndex which is not clone arg
    IndexRef srcIndex = new IndexRef("srcHost", "user", "pwd", "myIndex");
    IndexRef dstIndex = new IndexRef("dstHost", "user", "pwd", "myIndex");
    String[] cloneArguments = buildCloneArguments(srcIndex, dstIndex, "0", "4");

    int cloneArgumentsLength = cloneArguments.length/2; // key values pairs so /2
    assertEquals("Clone Args does not Match Options", optionsLength, cloneArgumentsLength);
  }

  @Test
  public void shouldThrowExceptionForInvalidArguments() throws ParseException {
    final String[] invalidArguments = {"this", "isInvalid"};
    exit.expectSystemExit();
    readCommandLine(invalidArguments);
  }

}