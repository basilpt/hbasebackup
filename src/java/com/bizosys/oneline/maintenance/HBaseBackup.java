package com.bizosys.oneline.maintenance;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseBackup {
	
	public static void main(String[] args) throws Exception{
		try {
			CommandLineArguments cla = new CommandLineArguments(args);
			String mode = cla.get("mode");
			System.out.println("Running Mode : " + mode);
			if ( "backup.full".equals(mode) ) {
				executeCompleteBackup(cla);
			} else if ( "backup.incremental".equals(mode) ) {
				executeIncrementalBackup(cla);
			} else if ( "backup.history".equals(mode) ) {
				listBackups(cla);
			} else if ( "backup.delete".equals(mode) ) {
				deleteBackups(cla);
			} else if ( "restore".equals(mode) ) {
				restore(cla);
			} else {
				showUsage();
			}
		} catch (Exception ex) {
			System.out.println(StringUtils.stringifyException(ex));
			throw ex;
		}
	}
	
	private static void listBackups(CommandLineArguments cla) throws Exception {
		String backupFolder = cla.get("backup.folder");
		if ( StringUtils.isEmpty(backupFolder)) {
			System.out.println("Backup area not found. Missing param backup.folder");
			showUsage();
			return;
		}

		if ( ! backupFolder.endsWith("/")) backupFolder = backupFolder + "/";
		Path path = new Path(backupFolder);
	    Configuration conf = HBaseConfiguration.create();
	    FileSystem s3fs = path.getFileSystem(conf);
	    if ( s3fs.exists(path) ) {
	    	System.out.println("Path exists : " + path.getName());
	    } else {
	    	System.out.println("Path Not Found : " + path.getName());
	    }
	    
		FileStatus[] dirs = s3fs.listStatus(path);
		if ( null == dirs) {
			System.out.println("No directories found under " + path.getName());
			return;
		}
		
		System.out.println("Incremental directories will have .incr at end");
		for (FileStatus dir : dirs) {
			String dirName = dir.getPath().getName();
			long size = dir.getLen();
			if ( StringUtils.isEmpty(dirName) ) continue;
			System.out.println("Backup Directory :" + dirName + " , size=" + size);
		}
	}
	
	private static void deleteBackups(CommandLineArguments cla) throws Exception {
		String backupFolder = cla.get("backup.folder");
		if ( StringUtils.isEmpty(backupFolder)) {
			System.out.println("Backup area not found. Missing param backup.folder");
			showUsage();
			return;
		}

		if ( ! backupFolder.endsWith("/")) backupFolder = backupFolder + "/";
		
		String strDate = cla.get("backup.date");
		System.out.println("Deleting the backup date as on : " + strDate);
		
		String backupFolderForDate = backupFolder + strDate;
		System.out.println("Deleting of sub directory : " + backupFolderForDate);
		
		
		Path path = new Path(backupFolderForDate);
	    Configuration conf = HBaseConfiguration.create();
	    
	    FileSystem s3fs = path.getFileSystem(conf);
	    Path testPath = new Path("/a");
	    if ( s3fs.exists(testPath ) ) {
	    	System.out.println("Path exists : " + path.getName());
	    } else {
	    	System.out.println("Path Not Found : " + testPath.getName());
	    }

		deleteFirectory(s3fs, path);
	    
	}

	private static void deleteFirectory(FileSystem s3fs, Path path) throws IOException {

		FileStatus[] dirs = s3fs.listStatus(path);
		if ( null == dirs) {
			System.out.println("No directories found under " + path.getName());
		    s3fs.delete(path, true); //Recursive delete
			return;
		}
		
		for (FileStatus dir : dirs) {
			String dirName = dir.getPath().getName();
			if ( StringUtils.isEmpty(dirName) ) continue;
			System.out.println("Deleting Sub Directory:" + dir.getPath().getName());
			if ( dir.isDir() ) deleteFirectory(s3fs, dir.getPath());
			else s3fs.delete(dir.getPath(), true); //Recursive delete
		}
	}

	private static void restore(CommandLineArguments cla) throws Exception {
		String backupFolder = cla.get("backup.folder");
		if ( StringUtils.isEmpty(backupFolder)) {
			System.out.println("Restore folder not found. Missing param backup.folder");
			showUsage();
			return;
		}

		if ( ! backupFolder.endsWith("/")) backupFolder = backupFolder + "/";

		String[] tables = StringUtils.getStrings(cla.get("tables"));
		int tablesT = ( null == tables) ? 0 : tables.length;
		System.out.println("Total tables to restore : " + tablesT);
		
		for ( int i=0; i< tablesT; i++) {
			String table = tables[i];
			String backupFolderForTable = backupFolder + table.replace("_", "underscore");
			System.out.println("Restoring : " + backupFolderForTable);
			invokeImport(table, backupFolderForTable);
		}
	}

	private static void invokeImport(String table, String backupFolderForTable) throws Exception {
		int retries = 0;
		while ( retries < 10) {
			try {
				Import.main(new String[]{table, backupFolderForTable});
				break;
			} catch (Exception ex) {
				System.out.println("Access Failure to " + backupFolderForTable + " , tries=" + retries);
				if ( retries == 10) throw ex;
				else {
					int interval = 1000 * (int) Math.pow(retries, 2);
					System.out.println("Sleeping for interval in millis " + interval );
					Thread.sleep(interval);
				}
			} finally {
				retries++;
			}
		}
	}	
	
	
	private static void executeCompleteBackup(CommandLineArguments cla) throws Exception {
		String backupFolder = cla.get("backup.folder");
		if ( StringUtils.isEmpty(backupFolder)) {
			showUsage();
			return;
		}
		
		Date atDate = null;
		String strDate = cla.get("date");
		if ( StringUtils.isEmpty(strDate)) {
			atDate = new Date();
		} else {
			atDate = parseDate(cla.get("date"));
		}
		
		System.out.println("Taking complete backup as on " + atDate.toString());

		String[] tables = StringUtils.getStrings(cla.get("tables"));
		int tablesT = ( null == tables) ? 0 : tables.length;
		System.out.println("Total tables to backup : " + tablesT);
		
		
		if ( 0 == tablesT ) {
			showUsage();
			return;
		}
		
		String backupFolderForDate = appendDate(backupFolder, atDate);
		System.out.println("Backing up at sub directory : " + backupFolderForDate);
		
		for ( int i=0; i< tablesT; i++) {
			String table = tables[i];
			String backupFolderForTable = backupFolderForDate + "/" + table.replace("_", "underscore");
			System.out.println("Backing up table: " + backupFolderForTable);
			
			invokeExport(table, backupFolderForTable, "0", new Long(atDate.getTime()).toString());
		}
	}

	private static String appendDate(String backupFolder, Date atDate) {
		backupFolder = ( backupFolder.endsWith("/")) ? 
			backupFolder : backupFolder + "/";
		String dt = atDate.toString();
		dt = dt.replace(" ", "_");
		dt = dt.replace(":", "_");
		String backupFolderForDate = backupFolder + dt;
		return backupFolderForDate;
	}
	
	private static void executeIncrementalBackup(CommandLineArguments cla) throws Exception {
		Date now = new Date();
		
		String backupFolder = cla.get("backup.folder");
		if ( StringUtils.isEmpty(backupFolder)) {
			showUsage();
			return;
		}
		
		long duration = 30;
		String strDuration = cla.get("duration.mins");
		if ( ! StringUtils.isEmpty(strDuration)) {
			duration = new Long(strDuration).longValue();
		}
		
		System.out.println("Taking incremental backup for last " + duration + " mins from now, " + now.toString() );

		String[] tables = StringUtils.getStrings(cla.get("tables"));
		int tablesT = ( null == tables) ? 0 : tables.length;
		System.out.println("Total tables to backup : " + tablesT);
		
		
		if ( 0 == tablesT ) {
			showUsage();
			return;
		}
		
		String backupFolderForDate = appendDate(backupFolder, now);
		System.out.println("Backing up at sub directory : " + backupFolderForDate);
		backupFolderForDate = backupFolderForDate + ".incr/";
		
		long end = now.getTime();
		long start = now.getTime() - duration;
		String strEnd = new Long(end).toString();
		String strStart = new Long(start).toString();

		for ( int i=0; i< tablesT; i++) {
			String table = tables[i];
			String backupFolderForTable = backupFolderForDate + table.replace("_", "underscore");
			System.out.println("Backing up table: " + backupFolderForTable);
			
			invokeExport(table, backupFolderForTable, strStart, strEnd );
		}
	}

	private static void invokeExport(String table, String backupFolderForTable, String strStart, String strEnd) throws Exception {
		int retries = 0;
		while ( retries < 10) {
			try {
				Export.main( new String[] { 
					table, backupFolderForTable,"1", strStart, strEnd});
				System.gc();
				
				break;
			} catch (Exception ex) {
				System.out.println("Access Failure to " + backupFolderForTable + " , tries=" + retries);
				if ( retries == 10) throw ex;
				else {
					int interval = 1000 * (int) Math.pow(retries, 2);
					System.out.println("Sleeping for interval in millis " + interval );
					Thread.sleep(interval);
				}
			} finally {
				retries++;
			}
		}
	}	

	private static void showUsage() {
		System.out.println("Usage: It runs in 4 modes as [backup.full], [backup.incremental], [backup.history] and [restore]");
		System.out.println("----------------------------------------");
		System.out.println("mode=backup.full tables=\"comma separated tables\" backup.folder=S3-Path  date=\"YYYY.MM.DD 24HH:MINUTE:SECOND:MILLSECOND TIMEZONE\"");
		System.out.println("\t\tEx. mode=backup.full tables=tab1,tab2,tab3 backup.folder=s3://S3BucketABC/ date=\"2011.12.01 17:03:38:546 IST\" ");
		System.out.println("\t\tEx. Default time is now\n mode=backup.full tables=tab1,tab2,tab3 backup.folder=s3://S3BucketABC/ ");
		System.out.println("----------------------------------------");
		System.out.println("mode=backup.incremental tables=\"comma separated tables\" backup.folder=S3-Path duration.mins=In Minutes ");
		System.out.println("\t\tEx. mode=backup.incremental backup.folder=s3://S3BucketABC/ duration.mins=30 tables=tab1,tab2,tab3");
		System.out.println("\t\tThis will backup changes happend in last 30 mins");
		System.out.println("----------------------------------------");
		System.out.println("mode=backup.history backup.folder=S3-Path");
		System.out.println("\t\tEx. mode=backup.history backup.folder=s3://S3BucketABC/");
		System.out.println("\t\tThis will list all past archieves. Incremental one ends with .incr");
		System.out.println("----------------------------------------");
		System.out.println("mode=restore  backup.folder=S3-Path/ArchieveDate tables=\"comma separated tables\"");
		System.out.println("\t\tEx. mode=backup.history backup.folder=s3://S3-Path/DAY_MON_HH_MI_SS_SSS_ZZZ_YYYY tables=tab1,tab2,tab3");
		System.out.println("\t\tThis will add the rows arcieved during that date. First apply a full backup and then apply incremental backups.");
	}

	/**
	 * Parse the date
	 * @param aDate
	 * @return
	 */
	private static Date parseDate(String aDate) throws ParseException{
		DateFormat df = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss:S z");
		Date date = df.parse(aDate);
		return date;
	}

}
