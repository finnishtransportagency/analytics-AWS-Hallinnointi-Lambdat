package com.amazonaws.lambda.vayla.sfn.ade;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class AdeCSVFilterer {
	
	AmazonS3 s3Client;
	LambdaLogger logger;
	private static final boolean DEBUG = true;
	
	public AdeCSVFilterer(AmazonS3 s3Client, LambdaLogger logger) {
		this.s3Client = s3Client;
		this.logger = logger;
	}
	
	private void log(String string) {
		if (logger == null)
			return;
		if (DEBUG)
			logger.log(string);
	}
	
	/**
	 * Public method for filtering a csv file aws s3, returns filtered rows gzipped and as a bytearray
	 * 
	 * @param bucket
	 * @param key
	 * @param csvHeaders
	 * @param tablefilter
	 * @return byte[]
	 */
	public byte[] filterFile(String bucket, String key, String[] csvHeaders, Tablefilter tablefilter) {
		List<Columnfilter> columnfilters = tablefilter.getFilters();
		
		// get csv data from s3
		List<CSVRecord> rows = getCSVRecords(bucket, key, csvHeaders);
		List<CSVRecord> filteredRows = new ArrayList<CSVRecord>();
		
		// Go through all csv rows
		for(CSVRecord row : rows) {
			boolean ok = false;
			// iterate through all given column and whitelisted values for table (per row)
			for(Columnfilter filter : columnfilters) {
				log("## column " + filter.getColumn());
				log("## row " + row.toString());
				// use filter value to get matching data from row
				String value = row.get(filter.getColumn());
				// null && empty are counted as whitelisted as well (since they are not sensitive data)
				if(value==null || value.isEmpty()) ok = true;
				// explicitly whitelisted value for column is ok
				if(filter.getValues().contains(value)) ok = true;
				log("## value " + value + " whitelisted " + ok);
			}
			if(ok) filteredRows.add(row);
		}
		
		// write filtered csv file back to s3
		return writeCSVRecords(bucket, key, filteredRows);
		
	}
	
	/**
	 * Writes csv records to stringbuffer, gzips them and returns as a bytearray
	 * for later streaming
	 * 
	 * @param bucket s3 bucket
	 * @param key s3 file key
	 * @param records csv data
	 */
	private byte[] writeCSVRecords(String bucket, String key, List<CSVRecord> records) {
		StringBuilder sb = new StringBuilder();
		byte[] gzipdata =  new byte[0];
		
		try {
			log("## printing csv");
			CSVPrinter printer = new CSVPrinter(sb, CSVFormat.EXCEL);
			for(CSVRecord rec : records) {
				printer.printRecord(rec);
			}
			
			gzipdata = GzipString.compressString(sb.toString());
			
			printer.close();
			log("## csv saved");			
		} catch (IOException e) {
			e.printStackTrace();
			logger.log("## Error writing file to S3: " + bucket + " , " + key);
		}
		
		return gzipdata;
	}
		
	/**
	 * Loads csv file from aws and parses the file to csv records
	 * 
	 * @param bucket aws s3 bucket name
	 * @param key the file key aka whole path in the bucket
	 * @return List<CSVRecord>
	 */
 	private List<CSVRecord> getCSVRecords(String bucket, String key, String[] headers) {
 		List<CSVRecord> records = new ArrayList<CSVRecord>();
 		
 		try {
 			log("## Downloading csv");
 			S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucket, key));
 			
 			S3ObjectInputStream s3in = fullObject.getObjectContent();
 			GZIPInputStream gzipin = new GZIPInputStream(s3in);
 			
 			BufferedReader reader = new BufferedReader(new InputStreamReader(gzipin));
 			CSVParser parser = CSVFormat.EXCEL.withHeader(headers).parse(reader); //CSVFormat.DEFAULT.withHeader(headers).withQuote(null).parse(reader);
 			records = parser.getRecords();
 			log("## returning " + records.size() + " csv records");

 		} catch (IOException e) {
 			e.printStackTrace();
 			logger.log("## Error reading file from S3: " + bucket + " , " + key);
 		}

 		return records;

 	}

}
