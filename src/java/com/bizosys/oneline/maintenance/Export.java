package com.bizosys.oneline.maintenance;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Export an HBase table.
 * Writes content to sequence files up in HDFS.  Use {@link Import} to read it
 * back in again.
 */
public class Export {
  private static final Log LOG = LogFactory.getLog(Export.class);
  final static String NAME = "export";

  /**
   * Mapper.
   */
  static class Exporter
  extends TableMapper<ImmutableBytesWritable, Result> {
    /**
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN,
     *   org.apache.hadoop.mapreduce.Mapper.Context)
     */
    @Override
    public void map(ImmutableBytesWritable row, Result value,
      Context context)
    throws IOException {
      try {
        context.write(row, value);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public static Job createSubmittableJob(Configuration conf, String[] args)
  throws IOException {
    String tableName = args[0];
    System.out.println("Path is:" + args[1].trim());
    Path outputDir = new Path(args[1].trim());
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJobName(NAME + "_" + tableName);
    job.setJarByClass(Exporter.class);
    // TODO: Allow passing filter and subset of rows/columns.
    Scan s = new Scan();
    // Optional arguments.
    int versions = args.length > 2? Integer.parseInt(args[2]): 1;
    s.setMaxVersions(versions);
    long startTime = args.length > 3? Long.parseLong(args[3]): 0L;
    long endTime = args.length > 4? Long.parseLong(args[4]): Long.MAX_VALUE;
    s.setTimeRange(startTime, endTime);
    s.setCacheBlocks(false);
    if (conf.get(TableInputFormat.SCAN_COLUMN_FAMILY) != null) {
      s.addFamily(Bytes.toBytes(conf.get(TableInputFormat.SCAN_COLUMN_FAMILY)));
    }
    LOG.info("verisons=" + versions + ", starttime=" + startTime +
      ", endtime=" + endTime);
    TableMapReduceUtil.initTableMapperJob(tableName, s, Exporter.class, null,
      null, job);
    // No reducers.  Just write straight to output files.
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Result.class);
    FileOutputFormat.setOutputPath(job, outputDir);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void usage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: Export [-D <property=value>]* <tablename> <outputdir> [<versions> " +
      "[<starttime> [<endtime>]]]\n");
    System.err.println("  Note: -D properties will be applied to the conf used. ");
    System.err.println("  For example: ");
    System.err.println("   -D mapred.output.compress=true");
    System.err.println("   -D mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec");
    System.err.println("   -D mapred.output.compression.type=BLOCK");
    System.err.println("  Additionally, the following SCAN properties can be specified");
    System.err.println("  to control/limit what is exported..");
    System.err.println("   -D " + TableInputFormat.SCAN_COLUMN_FAMILY + "=<familyName>");
  }

  static Configuration conf = HBaseConfiguration.create();
  
  /**
   * Main entry point.
   * table s3 repository.
   * @param args  The command line parameters. 
   *        content s3://hsearchbackup/ABC
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      usage("Wrong number of arguments: " + otherArgs.length);
      System.exit(-1);
    }
    Job job = createSubmittableJob(conf, otherArgs);
    int status =  job.waitForCompletion(true)? 0 : 1;
    String params = ( null == args) ? "" : StringUtils.arrayToString(args,'\t'); 
    if ( status == 1) {
    	String msg = "Error in Job completetion Params\n tablename \t outputdir \t versions \t start_time \t end_time\n " + params;
    	System.out.println(msg);
    }
    job.killJob();
  }
}
