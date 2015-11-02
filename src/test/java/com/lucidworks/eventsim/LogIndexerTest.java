package com.lucidworks.eventsim;

import org.junit.Ignore;
import org.junit.Test;

public class LogIndexerTest {

  @Ignore
  @Test
  public void test() throws Exception {
    String[] args = new String[]{
      "-dir", "src/test/resources/logs",
      "-zkHost", "localhost:9983",
      "-year", "2015",
      "-configName", "myconf",
      "-alreadyProcessedFiles","src/test/resources/already",
      "-deleteAfterIndexing", "false"
    };
    LogIndexer.main(args);
  }
}
