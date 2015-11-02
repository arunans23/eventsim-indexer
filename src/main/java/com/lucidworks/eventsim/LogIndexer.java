package com.lucidworks.eventsim;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.http.*;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.*;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.ShardParams;
import org.noggit.JSONParser;
import org.noggit.ObjectBuilder;

import com.codahale.metrics.MetricRegistry;


/**
 * Command-line utility for indexing eventsim data
 */
public class LogIndexer {

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  public static Logger log = Logger.getLogger(LogIndexer.class);

  private static final String ZK_HOST = "localhost:2181";

  static final SimpleDateFormat ISO_8601_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  static Option[] options() {
    return new Option[]{
      OptionBuilder
        .withArgName("PATH")
        .hasArg()
        .isRequired(true)
        .withDescription("Path to a directory containing logs")
        .create("dir"),
      OptionBuilder
        .withArgName("YYYY")
        .hasArg()
        .isRequired(false)
        .withDescription("Base year for log timestamps; defaults to 2015")
        .create("year"),
      OptionBuilder
        .withArgName("HOST")
        .hasArg()
        .isRequired(false)
        .withDescription("Address of the Zookeeper ensemble; defaults to: localhost:2181")
        .create("zkHost"),
      OptionBuilder
        .withArgName("FILE")
        .hasArg()
        .isRequired(false)
        .withDescription("Path to a file containing a list of files already processed")
        .create("alreadyProcessedFiles"),
      OptionBuilder
        .withArgName("SIZE")
        .hasArg()
        .isRequired(false)
        .withDescription("Size of the thread pool to process files in the directory; default is 10")
        .create("poolSize"),
      OptionBuilder
        .withArgName("DATE")
        .hasArg()
        .isRequired(false)
        .withDescription("Ignore any docs that occur before date/time")
        .create("ignoreBefore"),
      OptionBuilder
        .withArgName("NAME")
        .hasArg()
        .isRequired(false)
        .withDescription("Configuration to use when creating collections on-the-fly; must exist in ZooKeeper under /configs; default is sgconf2")
        .create("configName"),
      OptionBuilder
        .withArgName("NAME")
        .hasArg()
        .isRequired(false)
        .withDescription("Fusion URL; needed for creating collections on-the-fly")
        .create("fusionUrl"),
      OptionBuilder
        .withArgName("hourly|hash")
        .hasArg()
        .isRequired(false)
        .withDescription("Sharding schema, either hourly or hash based routing on the document ID; default is hash")
        .create("shardScheme"),
      OptionBuilder
        .withArgName("#")
        .hasArg()
        .isRequired(false)
        .withDescription("Number of shards to use when using the hash-based routing scheme; default is 48")
        .create("numShards"),
      OptionBuilder
        .withArgName("#")
        .hasArg()
        .isRequired(false)
        .withDescription("Batch size for indexing using the hash-based routing schema; default is 3000")
        .create("batchSize"),
      OptionBuilder
        .withArgName("#")
        .hasArg()
        .isRequired(false)
        .withDescription("Metrics reporting frequency; default is every 1 minute")
        .create("metricsReportingFrequency"),
      OptionBuilder
        .isRequired(false)
        .withDescription("Set this flag if you want to watch the directory for incoming files; this implies this application will run until killed")
        .create("watch"),
      OptionBuilder
        .withArgName("true|false")
        .hasArg()
        .isRequired(false)
        .withDescription("Delete files after indexing; default is true, pass false if you don't want to delete files.")
        .create("deleteAfterIndexing"),
      OptionBuilder
        .withArgName("COLLECTION")
        .hasArg()
        .isRequired(false)
        .withDescription("Name of existing collection")
        .create("collection")
    };
  }


  public static void main(String[] args) throws Exception {
    Options opts = getOptions();
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      System.err.println("Invalid command-line args!");
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LogIndexer.class.getName(), opts);
      System.exit(1);
    }
    LogIndexer app = new LogIndexer();
    app.run(processCommandLineArgs(opts, args));
  }

  protected int baseYear = 2015;
  protected ObjectMapper jsonMapper = new ObjectMapper();
  protected CloudSolrClient cloudClient;
  protected Map<String, SolrClient> leaderServers = new HashMap<String, SolrClient>();
  protected AtomicInteger docCounter = new AtomicInteger(0);
  protected AtomicInteger linesRead = new AtomicInteger(0);
  protected AtomicInteger parsedFiles = new AtomicInteger(0);
  protected AtomicInteger totalFiles = new AtomicInteger(0);
  protected long _startedAtMs = 0L;
  protected int poolSize = 10;
  protected File logDir;
  protected FileWriter processedFileSetWriter;
  protected Date ignoreBeforeDate;
  protected String configName = "sgconf2";
  protected String fusionUrl = "http://localhost:8765/api/v1/collections";
  protected String shardScheme;
  protected int numShards = 48;
  protected int batchSize = 3000;
  protected boolean watch = false;
  protected boolean deleteAfterIndexing = true;
  protected String fixedCollectionName = null;

  private static final MetricRegistry metrics = new MetricRegistry();
  private static final Timer sendBatchToSolrTimer = metrics.timer("sendBatchToSolr");
  private static final Counter docsIndexed = metrics.counter("numDocsIndexed");
  private static ConsoleReporter reporter = null;

  protected static Map<String, String> knownFields = new HashMap<String, String>();

  // from_s, subject_s, email_s, post_url_s, file_s
  static {
    knownFields.put("userid", "userid");
    knownFields.put("email", "email");
    knownFields.put("subject", "subject");
    knownFields.put("from", "from");
    knownFields.put("post_url", "post_url");
    knownFields.put("file", "file");
    knownFields.put("event", "event");
  }

  public void run(CommandLine cli) throws Exception {
    this.logDir = new File(cli.getOptionValue("dir"));
    if (!logDir.isDirectory())
      throw new FileNotFoundException(logDir.getAbsolutePath() + " not found!");

    File[] gzFiles = logDir.listFiles(new FileFilter() {
      public boolean accept(File f) {
        return f.isDirectory() || f.getName().endsWith(".gz");
      }
    });

    watch = cli.hasOption("watch");

    if (!watch) {
      if (gzFiles.length == 0) {
        log.error("No .gz log files found in " + logDir.getAbsolutePath());
        return;
      }
    }

    shardScheme = cli.getOptionValue("shardScheme", "hash");

    if ("hash".equals(shardScheme)) {
      numShards = Integer.parseInt(cli.getOptionValue("numShards","48"));
      batchSize = Integer.parseInt(cli.getOptionValue("batchSize","3000"));
      fixedCollectionName = cli.getOptionValue("collection");

      log.info("Configured to use hash-based document routing scheme");
    } else {
      numShards = 24;
      log.info("Configured to use implicit hourly document routing scheme");
    }

    if (cli.hasOption("ignoreBefore")) {
      ignoreBeforeDate = ISO_8601_DATE_FMT.parse(cli.getOptionValue("ignoreBefore"));
      log.info("Will ignore any log messages that occurred before: " + ignoreBeforeDate);
    }

    configName = cli.getOptionValue("configName", "sgconf2");

    if (cli.hasOption("fusionUrl")) {
      fusionUrl = cli.getOptionValue("fusionUrl");
    }

    deleteAfterIndexing = Boolean.parseBoolean(cli.getOptionValue("deleteAfterIndexing", "true"));

    baseYear = Integer.parseInt(cli.getOptionValue("year", "2015"));
    poolSize = Integer.parseInt(cli.getOptionValue("poolSize", "10"));
    jsonMapper = new ObjectMapper();

    if (reporter == null) {
      reporter = ConsoleReporter.forRegistry(metrics)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS).build();

      int metricsReportingFrequency = Integer.parseInt(cli.getOptionValue("metricsReportingFrequency", "1"));
      reporter.start(metricsReportingFrequency, TimeUnit.MINUTES);

      log.info("Started metrics console reporter to send reports every "+metricsReportingFrequency+" minutes");
    }

    String alreadyProcessedFiles = cli.getOptionValue("alreadyProcessedFiles");
    int restartAtLine = 0;

    String zkHost = cli.getOptionValue("zkHost", ZK_HOST);
    log.info("Connecting to Solr cluster: " + zkHost);
    try {
      cloudClient = new CloudSolrClient(zkHost);
      cloudClient.connect();
      log.info("Connected. Processing log files in " + logDir.getAbsolutePath());
      processLogDir(cloudClient, logDir, gzFiles, alreadyProcessedFiles, restartAtLine);
    } finally {

      if (leaderServers != null && !leaderServers.isEmpty()) {
        for (SolrClient solrServer : leaderServers.values()) {
          try {
            if (solrServer instanceof ConcurrentUpdateSolrClient) {
              ((ConcurrentUpdateSolrClient) solrServer).blockUntilFinished();
              ((ConcurrentUpdateSolrClient) solrServer).shutdownNow();
            }
          } catch (Exception exc) {
            log.warn("Error when shutting down leader server: " + solrServer);
          }
        }
      }

      if (cloudClient != null) {
        try {
          cloudClient.shutdown();
        } catch (Exception ignore) {
          ignore.printStackTrace();
        }
      }

      if (processedFileSetWriter != null) {
        try {
          processedFileSetWriter.flush();
          processedFileSetWriter.close();
        } catch (Exception exc) {
          log.error("Failed to close processed tracking file due to: " + exc);
        }
      }
    }
  }

  class DirectoryWatcherThread extends Thread {
    LogIndexer logIndexer;
    File dir;
    boolean keepRunning;
    ExecutorService pool;
    Set<File> knownDirectories = new HashSet<File>();
    Map<WatchKey,Path> keys = new HashMap<WatchKey,Path>();
    String collection;

    DirectoryWatcherThread(LogIndexer logIndexer, ExecutorService pool, File dir, String collection) {
      this.logIndexer = logIndexer;
      this.pool = pool;
      this.dir = dir;
      this.keepRunning = true;
      this.collection = collection;
    }

    public void run() {
      log.info("Directory watcher thread running for: "+logDir.getAbsolutePath());
      try {
        doRun();
      } catch (Exception exc) {
        log.error("Directory watcher on '"+dir.getAbsolutePath()+"' failed due to: "+exc, exc);
      }
    }

    protected void doRun() throws Exception {
      try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
        Path folder = Paths.get(logDir.getAbsolutePath());
        knownDirectories.add(logDir);
        WatchKey folderKey =
          folder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
        keys.put(folderKey, folder);

        File[] subDirs = logDir.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.isDirectory();
          }
        });

        for (File subDir : subDirs) {
          knownDirectories.add(subDir);
          Path subDirPath = Paths.get(subDir.getAbsolutePath());
          WatchKey subKey = subDirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
          keys.put(subKey, subDirPath);
        }

        while (keepRunning) {
          WatchKey key = watchService.take();
          if (key == null)
            continue;

          for(WatchEvent<?> watchEvent : key.pollEvents()) {
            if (StandardWatchEventKinds.ENTRY_CREATE == watchEvent.kind())
            {
              Path newPath = ((WatchEvent<Path>) watchEvent).context();
              if (newPath == null)
                continue;

              Path dirPath = keys.get(key);
              if (dirPath == null) {
                log.error("No path for key: "+key);
                continue;
              }

              File newFile = dirPath.resolve(newPath).toFile();
              if (newFile.isDirectory()) {
                if (!knownDirectories.contains(newFile)) {
                  knownDirectories.add(newFile);

                  log.info("New sub-directory detected: "+newFile.getAbsolutePath());

                  // register a watch on this new child directory
                  Path absPath = Paths.get(newFile.getAbsolutePath());
                  WatchKey subKey = absPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                  keys.put(subKey, absPath);

                  File[] inDir = newFile.listFiles(new FileFilter() {
                    public boolean accept(File pathname) {
                      return pathname.getName().endsWith(".gz");
                    }
                  });
                  for (File next : inDir) {
                    pool.submit(new FileParser(logIndexer, dir, next, 0, collection));
                    log.info("Scheduled new file '" + next + "' for parsing.");
                    totalFiles.incrementAndGet();
                  }
                }
              } else {
                pool.submit(new FileParser(logIndexer, dir, newFile, 0, collection));
                log.info("Scheduled new file '" + newFile + "' for parsing.");
                totalFiles.incrementAndGet();
              }
            }
          }

          if(!key.reset()) {
            log.error("WatchKey was reset!");
            break; //loop
          }
        }
      }
    }
  }

  protected List<File> getFilesToParse(File[] gzFiles) throws Exception {
    List<File> files = new ArrayList<File>();
    for (File f : gzFiles) {
      if (f.isDirectory()) {
        File[] inDir = f.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.getName().endsWith(".gz");
          }
        });

        for (File next : inDir)
          files.add(next);
      } else {
        files.add(f);
      }
    }
    Collections.sort(files, new Comparator<File>() {
      public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return files;
  }

  protected void processLogDir(CloudSolrClient cloudClient, File logDir, File[] gzFiles, String alreadyProcessedFiles, int restartAtLine) throws Exception {
    List<File> sortedGzFiles = getFilesToParse(gzFiles);

    log.info("Found "+sortedGzFiles.size()+" log files to parse");

    Set<String> alreadyProcessed = new HashSet<String>();
    if (alreadyProcessedFiles != null) {
      BufferedReader fr = null;
      String line = null;
      try {
        fr = new BufferedReader(new FileReader(alreadyProcessedFiles));
        while ((line = fr.readLine()) != null) {
          line = line.trim();
          if (line.length() > 0)
            alreadyProcessed.add(line);
        }
      } finally {
        if (fr != null) {
          try {
            fr.close();
          } catch (Exception ignore) {
          }
        }
      }
    }
    log.info("Found "+alreadyProcessed.size()+" already processed files.");

    processedFileSetWriter = new FileWriter(logDir.getName() + "_processed_files_v2", true);

    ExecutorService pool = Executors.newFixedThreadPool(poolSize);
    this._startedAtMs = System.currentTimeMillis();
    for (File file : sortedGzFiles) {
      String fileName = file.getAbsolutePath();
      if (alreadyProcessed.contains(fileName)) {
        log.info("Skipping already processed: " + fileName);
        continue;
      }
      pool.submit(new FileParser(this, logDir, file, 0, this.fixedCollectionName));
      totalFiles.incrementAndGet();
    }

    if (watch) {

      DirectoryWatcherThread watcherThread = new DirectoryWatcherThread(this, pool, logDir, this.fixedCollectionName);
      watcherThread.start();

      int numSleeps = 0;
      while (true) {
        ++numSleeps;
        try {
          Thread.sleep(60000);
        } catch (InterruptedException ie){
          Thread.interrupted();
        }

        // report progress every hour
        if (numSleeps % 60 == 0) {
          double _diff = (double) (System.currentTimeMillis() - _startedAtMs);
          long tookSecs = _diff > 1000 ? Math.round(_diff / 1000d) : 1;
          log.info("Processed " + parsedFiles.get() + " of " + totalFiles.get() + " files; running for: " + tookSecs +
            " (secs) to send " + (docCounter.get()) + " docs, read " + linesRead.get() +
            " lines; skipped: " + (linesRead.get() - docCounter.get()));
        }
      }
    } else {
      // wait for all queued work to complete
      shutdownAndAwaitTermination(pool);
      double _diff = (double) (System.currentTimeMillis() - _startedAtMs);
      long tookSecs = _diff > 1000 ? Math.round(_diff / 1000d) : 1;
      log.info("Processed " + parsedFiles.get() + " of " + totalFiles.get() + " files; took: " + tookSecs +
        " (secs) to send " + (docCounter.get()) + " docs, read " + linesRead.get() +
        " lines; skipped: " + (linesRead.get() - docCounter.get()));
    }
  }

  void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(96, TimeUnit.HOURS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  // Runs in a thread pool to parse files in-parallel
  class FileParser implements Runnable {

    private File logDir;
    private File fileToParse;
    private String fileName;
    private int skipOver;
    private LogIndexer logIndexer;
    private DateFormat dateParser;
    private Map<String,List<SolrInputDocument>> batchMap;
    private String collection;

    FileParser(LogIndexer logIndexer, File logDir, File fileToParse, int skipOver, String collection) {
      this.logIndexer = logIndexer;
      this.logDir = logDir;
      this.fileToParse = fileToParse;
      this.fileName = fileToParse.getAbsolutePath();
      this.skipOver = skipOver;
      this.dateParser = new SimpleDateFormat("yyyy-MMM dd HH:mm:ss");
      this.batchMap = "hash".equals(shardScheme) ? new HashMap<String,List<SolrInputDocument>>() : null;
      this.collection = collection;
    }

    public void run() {
      if (!fileToParse.isFile()) {
        log.warn("Skipping "+fileToParse.getAbsolutePath()+" because it doesn't exist anymore!");
        return;
      }
      try {
        doParseFile(fileToParse);
      } catch (Exception exc) {
        log.error("Failed to process file '" + fileName + "' due to: " + exc, exc);
      }
    }

    protected void doParseFile(File gzFile) throws Exception {
      BufferedReader br = null;
      String line = null;
      SolrInputDocument doc = null;
      int skippedLines = 0;
      String fileNameKey = fileName.substring(0, fileName.length() - 3);
      int lineNum = 0;

      long startMs = System.currentTimeMillis();

      try {
        br = new BufferedReader(
          new InputStreamReader(
            new GzipCompressorInputStream(
              new BufferedInputStream(
                new FileInputStream(gzFile))), StandardCharsets.UTF_8));

        if (log.isDebugEnabled())
          log.debug("Reading lines in file: " + fileName);

        while ((line = br.readLine()) != null) {
          logIndexer.linesRead.incrementAndGet();

          ++lineNum;

          if (lineNum < skipOver) {
            // support for skipping over lines
            continue;
          }

          line = line.trim();
          if (line.length() == 0)
            continue;

          doc = logIndexer.parseLogLine(logDir.getName(), fileNameKey, lineNum, line, dateParser);
          if (doc != null) {

            // CloudSolrClient is more efficient if you use batches vs. one-by-one
            if ("hash".equals(shardScheme)) {
              String coll = this.collection;
              if (coll == null) {
                String[] collectionAndShard = getTimeBasedShardForDoc(doc);
                coll = collectionAndShard[0];
              }
              List<SolrInputDocument> batch = batchMap.get(coll);
              if (batch == null) {
                batch = new ArrayList<SolrInputDocument>(batchSize);
                batchMap.put(coll, batch);
              }
              batch.add(doc);

              if (batch.size() >= batchSize) {
                logIndexer.indexBatch(batch, coll);
              }
            } else {
              logIndexer.indexDoc(doc);
            }

          } else {
            ++skippedLines;
          }

          if (lineNum > 200000) {
            if (lineNum % 20000 == 0) {
              long diffMs = System.currentTimeMillis() - startMs;
              log.info("Processed " + lineNum + " lines in " + fileName + "; running for " + diffMs + " ms");
            }
          }
        }

        // flush all docs in remaining batches
        for (String coll : batchMap.keySet()) {
          List<SolrInputDocument> batch = batchMap.get(coll);
          if (!batch.isEmpty()) {
            logIndexer.indexBatch(batch, coll);
          }
        }
        batchMap.clear();

      } catch (IOException ioExc) {
        log.error("Failed to process " + gzFile.getAbsolutePath() + " due to: " + ioExc);
      } finally {
        if (br != null) {
          try {
            br.close();
          } catch (Exception ignore) {
          }
        }
      }

      logIndexer.onFinishedParsingFile(fileName, lineNum, skippedLines, System.currentTimeMillis() - startMs);
    }
  }

  protected void onFinishedParsingFile(String fileName, int lineNum, int skippedLines, long tookMs) {
    log.info("Finished processing log file " + fileName + ", took " + tookMs + " ms; skipped " + skippedLines + " out of " + lineNum + " lines");

    int fileCounter = parsedFiles.incrementAndGet();
    int mod = fileCounter % 20;
    if (mod == 0) {
      log.info("Processed " + fileCounter + " of " + totalFiles.get() + " files so far");
    }

    synchronized (this) {
      if (processedFileSetWriter != null) {
        try {
          processedFileSetWriter.append(fileName);
          processedFileSetWriter.append('\n');
          processedFileSetWriter.flush();
        } catch (IOException ioexc) {
          log.error("Failed to write " + fileName + " into processed files list due to: " + ioexc);
        }
      }
    }

    if (deleteAfterIndexing) {
      File toDelete = new File(fileName);
      if (toDelete.isFile()) {
        toDelete.delete();
      }
    }
  }

  protected void indexBatch(List<SolrInputDocument> batch, String collection) throws Exception {
    String[] collectionAndShard = new String[2];
    collectionAndShard[0] = collection;
    SolrClient leader = getLeaderSolrClient(collectionAndShard);

    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collection);
    req.setParam(UpdateRequest.OVERWRITE, "false");
    //req.setCommitWithin(100);
    req.add(batch);

    sendBatch(cloudClient, req, 3, 2, true);
    docCounter.addAndGet(batch.size());
    docsIndexed.inc(batch.size());
    batch.clear();
  }

  protected void sendBatch(CloudSolrClient cloudSolrClient,
                          UpdateRequest batchRequest,
                          int waitBeforeRetry,
                          int maxRetries,
                          boolean withTimer)
    throws Exception
  {
    final Timer.Context sendTimerCtxt = (withTimer) ? sendBatchToSolrTimer.time() : null;
    try {
      cloudSolrClient.request(batchRequest);
    } catch (Exception exc) {

      Throwable rootCause = SolrException.getRootCause(exc);
      boolean wasCommError =
        (rootCause instanceof ConnectException ||
          rootCause instanceof ConnectTimeoutException ||
          rootCause instanceof NoHttpResponseException ||
          rootCause instanceof SocketException);

      if (wasCommError) {
        if (--maxRetries > 0) {
          log.warn("ERROR: "+rootCause+" ... Sleeping for "+waitBeforeRetry+" seconds before re-try ...");
          Thread.sleep(waitBeforeRetry*1000L);
          sendBatch(cloudSolrClient, batchRequest, waitBeforeRetry, maxRetries, false);
        } else {
          log.error("No more retries available! Add batch failed due to: "+rootCause);
          throw exc;
        }
      }
    } finally {
      if (sendTimerCtxt != null) {
        sendTimerCtxt.stop();
      }
    }
  }


  protected void indexDoc(SolrInputDocument doc) throws Exception {
    String[] collectionAndShard = getTimeBasedShardForDoc(doc);
    SolrClient leader = getLeaderSolrClient(collectionAndShard);

    UpdateRequest req = new UpdateRequest();
    req.setParam("collection", collectionAndShard[0]);

    if (collectionAndShard[1] != null) {
      req.setParam(ShardParams._ROUTE_, collectionAndShard[1]);
    }

    req.add(doc);

    try {
      leader.request(req);
    } catch (Exception ie) {
      log.warn("CUSS queue is backed up ... waiting 2 secs before retrying the update request");

      if (leader instanceof ConcurrentUpdateSolrClient) {
        ((ConcurrentUpdateSolrClient) leader).blockUntilFinished();
      }

      try {
        Thread.sleep(2000);
      } catch (Exception ignore) {}
      leader.request(req);
    }
    docCounter.incrementAndGet();
  }

  protected SolrClient getLeaderSolrClient(String[] collectionAndShard) throws Exception {
    String leaderKey = collectionAndShard[0] + " / " + collectionAndShard[1];
    SolrClient leader = null;
    synchronized (leaderServers) {
      leader = leaderServers.get(leaderKey);
      if (leader == null) {
        ZkStateReader zkStateReader = cloudClient.getZkStateReader();

        if (!zkStateReader.getClusterState().hasCollection(collectionAndShard[0])) {
          // go create the collection on-the-fly
          try {
            createCollection(collectionAndShard[0], numShards, 2, configName);
            Thread.sleep(2000); // give some time for the collection to activate
          } catch (Exception exc) {
            // maybe already created by another instance of this app
            // so re-check for existing before failing ...
            zkStateReader.updateClusterState(true);
            if (!zkStateReader.getClusterState().hasCollection(collectionAndShard[0])) {
              log.error("Failed to create collection '"+collectionAndShard[0]+"' due to: "+exc);
              throw exc;
            }
          }

          try {
            createFusionCollection(fusionUrl, "admin", "S3ndGr1d15", collectionAndShard[0]);
          } catch (Exception exc) {
            // not the end of the world if this happens
            log.error("Failed to create Fusion collection '"+collectionAndShard[0]+"' due to: "+exc);
          }
        }

        try {

          if ("hash".equals(shardScheme)) {
            leader = cloudClient;
          } else {
            String leaderUrl = zkStateReader.getLeaderUrl(collectionAndShard[0], collectionAndShard[1], 5000);
            leader = getCUSS(leaderKey, leaderUrl);
          }
          leaderServers.put(leaderKey, leader);
        } catch (org.apache.solr.common.SolrException se) {
          log.error("Failed to get leader for collection " +
            collectionAndShard[0] + " shard " + collectionAndShard[1] + " due to: " + se + "; trying again after updating ClusterState");
          zkStateReader.updateClusterState(true);

          if ("hash".equals(shardScheme)) {
            leader = cloudClient;
          } else {
            String leaderUrl = zkStateReader.getLeaderUrl(collectionAndShard[0], collectionAndShard[1], 5000);
            leader = getCUSS(leaderKey, leaderUrl);
          }
          leaderServers.put(leaderKey, leader);
        }
      }
    }
    return leader;
  }

  protected void createCollection(String collName, int numShards, int replicationFactor, String configName) throws Exception {
    Set<String> liveNodes = cloudClient.getZkStateReader().getClusterState().getLiveNodes();
    if (liveNodes.isEmpty())
      throw new IllegalStateException("No live nodes found! Cannot create a collection until " +
        "there is at least 1 live node in the cluster.");
    String firstLiveNode = liveNodes.iterator().next();

    String baseUrl = cloudClient.getZkStateReader().getBaseUrlForNodeName(firstLiveNode);

    int numNodes = liveNodes.size();
    int maxShardsPerNode = ((numShards*replicationFactor)+numNodes-1)/numNodes;

    String shards = "hash".equals(shardScheme) ? "numShards="+numShards
      : "router.name=implicit&shards=h01,h02,h03,h04,h05,h06,h07,h08,h09,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22,h23,h24";

    String createCollectionUrl =
      String.format(Locale.ROOT,
        "%s/admin/collections?action=CREATE&name=%s&replicationFactor=%d&maxShardsPerNode=%d&collection.configName=%s&%s",
              baseUrl,
              collName,
              replicationFactor,
              maxShardsPerNode,
        configName,
              shards);

    log.info("Creating new collection '" + collName + "' using command: " + createCollectionUrl);

    getJson(createCollectionUrl);

  }

  protected ConcurrentUpdateSolrClient getCUSS(final String leaderKey, final String leaderUrl) {
    ConcurrentUpdateSolrClient server = new ConcurrentUpdateSolrClient(leaderUrl, 5000, 2) {
      @Override
      public void handleError(Throwable ex) {
        log.error("Request to '" + leaderUrl + "' failed due to: " + ex);
        synchronized (leaderServers) {
          leaderServers.remove(leaderKey);
        }
      }
    };

    server.setParser(new BinaryResponseParser());
    server.setRequestWriter(new BinaryRequestWriter());
    server.setPollQueueTime(20);

    return server;
  }

  protected String[] months = new String[]{
    "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
  };

  protected String[] getTimeBasedShardForDoc(SolrInputDocument doc) {
    Date timestamp_tdt = (Date) doc.getFieldValue("timestamp_tdt");
    Calendar cal = Calendar.getInstance();
    cal.setTime(timestamp_tdt);
    String monthKey = months[cal.get(Calendar.MONTH)];

    String[] collectionAndShard = new String[2];

    int hourOfDay = cal.get(Calendar.HOUR_OF_DAY);
    int bucket = 1 + hourOfDay;

    String hourShard = bucket < 10 ? "0" + bucket : String.valueOf(bucket);

    int dayOfMonth = cal.get(Calendar.DAY_OF_MONTH);

    String day = dayOfMonth < 10 ? "0" + dayOfMonth : String.valueOf(dayOfMonth);
    collectionAndShard[0] = "logs_" + monthKey + day; // +"_"+(cal.get(Calendar.YEAR)-2000);

    // shard is null for hash-based routing
    collectionAndShard[1] = "hash".equals(shardScheme) ? null : "h" + hourShard;

    return collectionAndShard;
  }

  protected SolrInputDocument parseLogLine(String dirName, String fileName, int lineNum, String line, DateFormat dateParser) {

    // TODO: minimal line length
    if (line.length() < 20) {
      log.error("Ignoring line " + lineNum + " in " + fileName + " due to: line is too short; " + line);
      return null;
    }

    SolrInputDocument doc = new SolrInputDocument();

    int pos = 0;
    String toFind = "{";
    int at = line.indexOf(toFind, pos);
    if (at == -1) {
      doc.setField("json", line);
      doc.setField("invalid_json_b", Boolean.TRUE);
      return doc;
    }

    String json = line;
    if (!"{}".equals(json)) {
      if (json.endsWith("'") || json.endsWith("\""))
        json = json.substring(0, json.length() - 1);

      try {
        addJsonObjectFieldsToDoc(jsonMapper.readTree(json), null, doc);
      } catch (Exception exc) {
          log.warn("Failed to parse JSON at line " + lineNum + " in " + fileName + " due to: " + exc);
        doc.setField("invalid_json_b", Boolean.TRUE);
      }
    }

    Object tsObj = doc.getFieldValue("ts_l");
    if (tsObj == null) {
      doc.setField("json", line);
      doc.setField("invalid_json_b", Boolean.TRUE);
      return doc;
    }

    Date timestampDate = asDate(tsObj);
    if (ignoreBeforeDate != null && timestampDate.before(ignoreBeforeDate))
      return null;

    String docId = timestampDate.getTime() + "_" + fileName + "_" + lineNum;
    doc.setField("id", docId);
    doc.setField("timestamp_tdt", timestampDate);

    Object reg = doc.getFieldValue("registration_l");
    if (reg != null) {
      Date regDate = asDate(reg);
      if (regDate != null) {
        doc.removeField("registration_l");
        doc.setField("registration_tdt", regDate);
      }
    }

    return doc;
  }

  protected Date asDate(Object tsObj) {
    //
    long tsLong = (tsObj instanceof Number) ? ((Number)tsObj).longValue() : Long.parseLong(tsObj.toString());
    return new Date(tsLong);
  }

  protected void addJsonObjectFieldsToDoc(JsonNode json, String namePrefix, SolrInputDocument doc) throws Exception {
    Iterator<String> fIter = json.fieldNames();
    while (fIter.hasNext()) {
      String fname = fIter.next();
      JsonNode node = json.get(fname);
      if (node == null)
        continue;

      boolean isContainerNode = node.isContainerNode();

      // first, handle top-level known fields
      if (namePrefix == null) {
        if (knownFields.containsKey(fname) && !isContainerNode) {
          addKnownField(fname, node, doc);
          continue;
        }

        if ((fname.equals("timing") || fname.indexOf("timing") != -1) && node.isArray()) {
          boolean wasHandled = false;
          for (int v = 0; v < node.size(); v++) {
            JsonNode elm = node.get(v);
            if (elm.isObject()) {
              JsonNode time = elm.get("time");
              JsonNode state = elm.get("state");
              if (time != null && state != null) {
                doc.setField(fname + "_" + state.asText() + "_f", time.floatValue());
                wasHandled = true;
              } else {
                JsonNode total = elm.get("total");
                if (total != null) {
                  doc.setField(fname + "_total_f", total.floatValue());
                  wasHandled = true;
                }
              }
            }
          }

          if (wasHandled)
            continue;
        }

        if ("processed".equals(fname)) {
          try {
            long processedSecs = Long.parseLong(node.asText());
            doc.addField("processed_tdt", new Date(processedSecs * 1000L));
            continue;
          } catch (NumberFormatException nfe) {
          }
        }

        // anything that looks like a time, but not a timestamp_tdt
        if ("time".equals(fname) || (fname.indexOf("time") != -1 && fname.indexOf("timestamp_tdt") == -1)) {
          try {
            Float timeF = new Float(node.asText());
            doc.addField(fname + "_f", timeF);
            continue;
          } catch (NumberFormatException nfe) {
          }
        }

        if ("response_code".equals(fname)) {
          try {
            Integer code = new Integer(node.asText());
            doc.addField("response_code_i", code);
            continue;
          } catch (NumberFormatException nfe) {
          }
        }

        if ("has_verdicts".equals(fname)) {
          try {
            Boolean b = new Boolean(node.asText());
            doc.addField("has_verdicts_b", b);
            continue;
          } catch (Exception nfe) {
          }
        }

      }

      // unknown fields - treat as dynamic Solr fields

      // for nested objects support
      if (namePrefix != null)
        fname = namePrefix + "_" + fname;

      if (isContainerNode) {
        if (node.isArray()) {
          // TODO: treating all multi-valued fields as strings
          for (int v = 0; v < node.size(); v++) {
            doc.addField(fname + "_ss", node.get(v).asText());
          }
        } else {
          addJsonObjectFieldsToDoc(node, fname, doc);
        }
      } else {
        String solrFieldName = getSolrFieldNameForSingleValuedNode(node, fname);
        Object value = getFieldValueForSingleValuedNode(node);
        if (value != null)
          doc.addField(solrFieldName, value);
      }
    }
  }

  protected void addKnownField(String fname, JsonNode node, SolrInputDocument doc) {
    String asText = node.asText();
    if (asText != null && asText.length() > 0)
      doc.addField(knownFields.get(fname), asText);
  }

  protected String getSolrFieldNameForSingleValuedNode(JsonNode node, String baseName) {
    baseName = baseName.replace('-', '_').replace(' ', '_');
    if (node.isNumber()) {
      if (node.isIntegralNumber()) {
        return baseName + "_l"; // long
      } else if (node.isFloatingPointNumber()) {
        return baseName + "_d"; // double
      }
    } else if (node.isBoolean()) {
      return baseName + "_b";
    }
    return baseName + "_s"; // default it to String
  }

  protected Object getFieldValueForSingleValuedNode(JsonNode node) {
    if (node.isNumber()) {
      if (node.isIntegralNumber()) {
        return node.asLong();
      } else if (node.isFloatingPointNumber()) {
        return node.asDouble();
      }
    } else if (node.isBoolean()) {
      return node.asBoolean();
    }
    String str = node.asText();
    return (str != null && str.length() == 0) ? null : str;
  }

  static void displayOptions(PrintStream out) throws Exception {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(LogIndexer.class.getName(), getOptions());
  }

  static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = options();
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }

  public static CommandLine processCommandLineArgs(Options options, String[] args) {
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LogIndexer.class.getName(), options);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LogIndexer.class.getName(), options);
      System.exit(0);
    }

    return cli;
  }

  public static final Throwable getRootCause(Throwable thr) {
    if (thr == null)
      return null;
    Throwable rootCause = thr;
    Throwable cause = thr;
    while ((cause = cause.getCause()) != null)
      rootCause = cause;
    return rootCause;
  }

  public void createFusionCollection(String fusionUrl, String fusionUser, String fusionPass, String collName) {
    HttpClient httpClient = getHttpClient();
    try {
      if (!(httpClient instanceof DefaultHttpClient))
        throw new IllegalStateException("Expected DefaultHttpClient but got "+
          (httpClient.getClass().getName())+" instead!");

      DefaultHttpClient dhc = (DefaultHttpClient)httpClient;
      dhc.addRequestInterceptor(new PreEmptiveBasicAuthenticator(fusionUser, fusionPass));

      // curl -u admin:S3ndGr1d15 -X POST -H 'Content-type: application/json' -d '{"id":"logs_$day","solrParams":{"name":"logs_$day"}}' 

      String postBodyJson = "{\"id\":\""+collName+"\",\"solrParams\":{\"name\":\""+collName+"\"}}";

      // Prepare a request object
      HttpPost httpPost = new HttpPost(fusionUrl);

      StringEntity params =new StringEntity(postBodyJson);
      httpPost.addHeader("content-type", "application/json");
      httpPost.setEntity(params);

      // Execute the request
      log.info("Creating Fusion collection '"+collName+"' by POSTing "+postBodyJson+" to: "+fusionUrl);
      HttpResponse response = httpClient.execute(httpPost);

      // Get hold of the response entity
      HttpEntity entity = response.getEntity();
      if (response.getStatusLine().getStatusCode() != 200) {
        StringBuilder body = new StringBuilder();
        if (entity != null) {
          InputStream instream = entity.getContent();
          String line;
          try {
            BufferedReader reader =
              new BufferedReader(new InputStreamReader(instream, "UTF-8"));
            while ((line = reader.readLine()) != null) {
              body.append(line);
            }
          } catch (Exception ignore) {
            // squelch it - just trying to compose an error message here
          } finally {
            instream.close();
          }
        }
        throw new Exception("Create Fusion collection in [" + fusionUrl + "] failed due to: "
          + response.getStatusLine() + ": " + body);
      }


    } catch (Exception exc) {
      log.error("Failed to create Fusion collection '"+collName+"' due to: "+exc, exc);
    } finally {
      closeHttpClient(httpClient);
    }
  }

  public static Map<String,Object> getJson(String getUrl) throws Exception {
    Map<String,Object> json = null;
    HttpClient httpClient = getHttpClient();
    try {
      json = getJson(httpClient, getUrl, 2);
    } finally {
      closeHttpClient(httpClient);
    }
    return json;
  }

  /**
   * Utility function for sending HTTP GET request to Solr with built-in retry support.
   */
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl, int attempts) throws Exception {
    Map<String,Object> json = null;
    if (attempts >= 1) {
      try {
        json = getJson(httpClient, getUrl);
      } catch (Exception exc) {
        if (--attempts > 0 && checkCommunicationError(exc)) {
          log.warn("Request to "+getUrl+" failed due to: "+exc.getMessage()+
            ", sleeping for 5 seconds before re-trying the request ...");
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ie) { Thread.interrupted(); }

          // retry using recursion with one-less attempt available
          json = getJson(httpClient, getUrl, attempts);
        } else {
          // no more attempts or error is not retry-able
          throw exc;
        }
      }
    }

    return json;
  }

  /**
   * Utility function for sending HTTP GET request to Solr and then doing some
   * validation of the response.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Map<String,Object> getJson(HttpClient httpClient, String getUrl) throws Exception {
    Map<String,Object> json = null;

    // ensure we're requesting JSON back from Solr
    URL url = new URL(getUrl);
    String queryString = url.getQuery();
    if (queryString != null) {
      if (queryString.indexOf("wt=json") == -1) {
        getUrl += "&wt=json";
      }
    } else {
      getUrl += "?wt=json";
    }

    // Prepare a request object
    HttpGet httpget = new HttpGet(getUrl);

    // Execute the request
    HttpResponse response = httpClient.execute(httpget);

    // Get hold of the response entity
    HttpEntity entity = response.getEntity();
    if (response.getStatusLine().getStatusCode() != 200) {
      StringBuilder body = new StringBuilder();
      if (entity != null) {
        InputStream instream = entity.getContent();
        String line;
        try {
          BufferedReader reader =
            new BufferedReader(new InputStreamReader(instream, "UTF-8"));
          while ((line = reader.readLine()) != null) {
            body.append(line);
          }
        } catch (Exception ignore) {
          // squelch it - just trying to compose an error message here
        } finally {
          instream.close();
        }
      }
      throw new Exception("GET request [" + getUrl + "] failed due to: "
        + response.getStatusLine() + ": " + body);
    }

    // If the response does not enclose an entity, there is no need
    // to worry about connection release
    if (entity != null) {
      InputStreamReader isr = null;
      try {
        isr = new InputStreamReader(entity.getContent(), "UTF-8");
        Object resp =
          ObjectBuilder.getVal(new JSONParser(isr));
        if (resp != null && resp instanceof Map) {
          json = (Map<String,Object>)resp;
        } else {
          throw new SolrServerException("Expected JSON object in response from "+
            getUrl+" but received "+ resp);
        }
      } catch (RuntimeException ex) {
        // In case of an unexpected exception you may want to abort
        // the HTTP request in order to shut down the underlying
        // connection and release it back to the connection manager.
        httpget.abort();
        throw ex;
      } finally {
        // Closing the input stream will trigger connection release
        isr.close();
      }
    }

    // lastly check the response JSON from Solr to see if it is an error
    int statusCode = -1;
    Map responseHeader = (Map)json.get("responseHeader");
    if (responseHeader != null) {
      Long status = (Long)responseHeader.get("status");
      if (status != null)
        statusCode = status.intValue();
    }

    if (statusCode == -1)
      throw new SolrServerException("Unable to determine outcome of GET request to: "+
        getUrl+"! Response: "+json);

    if (statusCode != 0) {
      String errMsg = null;
      Map error = (Map) json.get("error");
      if (error != null) {
        errMsg = (String)error.get("msg");
      }

      if (errMsg == null) errMsg = String.valueOf(json);
      throw new SolrServerException("Request to "+getUrl+" failed due to: "+errMsg);
    }

    return json;
  }

  /**
   * Helper function for reading a String value from a JSON Object tree.
   */
  public static String asString(String jsonPath, Map<String,Object> json) {
    String str = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof String) {
        str = (String)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a String at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return str;
  }

  /**
   * Helper function for reading a Long value from a JSON Object tree.
   */
  public static Long asLong(String jsonPath, Map<String,Object> json) {
    Long num = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof Long) {
        num = (Long)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a Long at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return num;
  }

  /**
   * Helper function for reading a List of Strings from a JSON Object tree.
   */
  @SuppressWarnings("unchecked")
  public static List<String> asList(String jsonPath, Map<String,Object> json) {
    List<String> list = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof List) {
        list = (List<String>)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a List at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return  list;
  }

  /**
   * Helper function for reading a Map from a JSON Object tree.
   */
  @SuppressWarnings("unchecked")
  public static Map<String,Object> asMap(String jsonPath, Map<String,Object> json) {
    Map<String,Object> map = null;
    Object obj = atPath(jsonPath, json);
    if (obj != null) {
      if (obj instanceof Map) {
        map = (Map<String,Object>)obj;
      } else {
        // no ok if it's not null and of a different type
        throw new IllegalStateException("Expected a Map at path "+
          jsonPath+" but found "+obj+" instead! "+json);
      }
    } // it's ok if it is null
    return map;
  }

  /**
   * Helper function for reading an Object of unknown type from a JSON Object tree.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Object atPath(String jsonPath, Map<String,Object> json) {
    if ("/".equals(jsonPath))
      return json;

    if (!jsonPath.startsWith("/"))
      throw new IllegalArgumentException("Invalid JSON path: " +
        jsonPath + "! Must start with a /");

    Map<String, Object> parent = json;
    Object result = null;
    String[] path = jsonPath.split("/");
    for (int p = 1; p < path.length; p++) {
      Object child = parent.get(path[p]);
      if (child == null)
        break;

      if (p == path.length - 1) {
        // success - found the node at the desired path
        result = child;
      } else {
        if (child instanceof Map) {
          // keep walking the path down to the desired node
          parent = (Map) child;
        } else {
          // early termination - hit a leaf before the requested node
          break;
        }
      }
    }
    return result;
  }

  /**
   * Determine if a request to Solr failed due to a communication error,
   * which is generally retry-able.
   */
  public static boolean checkCommunicationError(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    boolean wasCommError =
      (rootCause instanceof ConnectException ||
        rootCause instanceof ConnectTimeoutException ||
        rootCause instanceof NoHttpResponseException ||
        rootCause instanceof SocketException);
    return wasCommError;
  }

  public static HttpClient getHttpClient() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS, 128);
    params.set(HttpClientUtil.PROP_MAX_CONNECTIONS_PER_HOST, 32);
    params.set(HttpClientUtil.PROP_FOLLOW_REDIRECTS, false);
    return HttpClientUtil.createClient(params);
  }

  @SuppressWarnings("deprecation")
  public static void closeHttpClient(HttpClient httpClient) {
    if (httpClient != null) {
      try {
        httpClient.getConnectionManager().shutdown();
      } catch (Exception exc) {
        // safe to ignore, we're just shutting things down
      }
    }
  }

  private final class PreEmptiveBasicAuthenticator implements HttpRequestInterceptor {
    private final UsernamePasswordCredentials credentials;

    public PreEmptiveBasicAuthenticator(String user, String pass) {
      credentials = new UsernamePasswordCredentials(user, pass);
    }

    @Override
    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      request.addHeader(BasicScheme.authenticate(credentials, "US-ASCII", false));
    }
  }

}
