package com.amazonaws.lambda.vayla.sfn.ade;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.kms.model.NotFoundException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;

/**
 * Gets a json object describing manifest file and relating data file locations
 * 1. Loads a whitelist from given location
 * 2. Goes through csv data files (manifest includes information about columns) and
 * filters out all rows with non-whitelisted data
 * 3. Outputs information of manifest and data files
 */
public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	private static final boolean DEBUG = true;
	LambdaLogger logger = null;
	AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
	static final String whitelistKey = System.getenv("whitelistkey"); 
	static final String s3WorkBucket = System.getenv("S3_bucket");

	private void log(String string) {
		if (logger == null)
			return;
		if (DEBUG)
			logger.log(string);
	}

    @Override
    public String handleRequest(Object input, Context context) {
    	String output = "{}";
    	this.logger = context.getLogger();
        log("Input: " + input);
        
        try {
        	// parse inputjson for jsonpath
			String inputstring =  (String)input; 
			log("## inputjson: " + inputstring);
			Object inputJson = Configuration.defaultConfiguration().jsonProvider().parse(inputstring);
			
			// find manifest file key
			String manifestkey = JsonPath.read(inputJson, "$.manifestfile");
			log("## manifestkey: " + manifestkey);
			
			// load manifest file
			String manifestString = getObject(s3WorkBucket,manifestkey);
			Object manifestJson = Configuration.defaultConfiguration().jsonProvider().parse(manifestString);
			
			// get whitelist json from given bucket
			String whitelistedString = getObject(s3WorkBucket, whitelistKey);
			//log("## Whitelist string: " + whitelistedString);
			
			// check if manifest file has an entry in whitelist
			Object whitelistJson = Configuration.defaultConfiguration().jsonProvider().parse(whitelistedString);
			List<String> whitelistedFiles = JsonPath.read(whitelistJson, "$.tables[*].tablename");
			Tablefilter tablefilter = null;
			int i=0;
			for (String target : whitelistedFiles) {
				if(manifestkey.contains(target)) {
					log("## target file/table found: " + target);
					tablefilter = JsonPath.parse(whitelistJson).read("$.tables[" +i + "]", Tablefilter.class);
					break;
				}
				i++;
			}
			
			// check if we have a valid table filter - stop execution if not
			if(tablefilter!=null) {
				log("## created tablefilter for: " + tablefilter.getTablename());
			} else {
				throw new NotFoundException("Could not find filter for " + manifestkey);
			}
			
			// get headers for target files
			List<String> csvHeadersList = JsonPath.read(manifestJson, "$.columns[*]");
			String[] csvHeaders = new String[csvHeadersList.size()];
			csvHeaders = csvHeadersList.toArray(csvHeaders);
			
			// find target file key(s)
			List<String> targetfilekeys = JsonPath.read(inputJson, "$.datafiles[*]");
			log("## targetkeys: " + targetfilekeys.size());
			
			// filter files and save them
			AdeCSVFilterer adeCSVFilterer = new AdeCSVFilterer(s3Client, logger);
			for(String targetfilekey : targetfilekeys) {
				byte[] data = adeCSVFilterer.filterFile(s3WorkBucket, targetfilekey, csvHeaders, tablefilter);
				saveObject(s3WorkBucket, targetfilekey, data);
			}
			
			// TODO: generate output with additional information?
			// for now we just pass on the input with info about manifest + target file(s)
			output = inputstring;
			
		} catch (InvalidJsonException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
        
        return output;
    }
    
    // Save given byte array data to S3
    private void saveObject(String bucket, String key, byte[] data) {
    	try {
    		log("## Saving an object");
    		ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
    		ObjectMetadata objectMetadata = new ObjectMetadata();
    		objectMetadata.setContentLength(data.length);
    		s3Client.putObject(bucket, key, byteIn, objectMetadata);
    	} catch(SdkClientException e) {
    		e.printStackTrace();
    		logger.log("## Error saving file to S3 " + bucket + ", " + key);
    	} 
    	
    }
    
    // Reads json file from given bucket and returns it as a string
 	private String getObject(String bucket, String key) {
 		StringBuffer sb = new StringBuffer();
 		try {
 			log("## Downloading an object");
 			S3Object fullObject = s3Client.getObject(new GetObjectRequest(bucket, key));
 			log("## Content-Type: " + fullObject.getObjectMetadata().getContentType());
 			
 			BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
 			String line = null;
 			while ((line = reader.readLine()) != null) {
 				sb.append(line);
 			}
 		} catch (IOException e) {
 			e.printStackTrace();
 			logger.log("## Error reading file from S3: " + bucket + " ," + key);
 		}

 		return sb.toString();

 	}

}
