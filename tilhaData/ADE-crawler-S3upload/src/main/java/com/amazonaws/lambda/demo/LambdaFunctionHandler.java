package com.amazonaws.lambda.demo;

import java.util.List;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	private static final boolean DEBUG = true;
	LambdaLogger logger = null;
	AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
	static final String s3SourceBucket = System.getenv("S3_bucket_source");
	static final String s3TargetBucket = System.getenv("S3_bucket_target");

	
	private void log(String string) {
		if (logger == null)
			return;
		if (DEBUG)
			logger.log(string);
	}

	/**
	 * Get a json with information about manifest and related data files.
	 * Copies them from given source S3 bucket to given target S3 bucket.
	 */
    @Override
    public String handleRequest(Object input, Context context) {
    	this.logger = context.getLogger();
        log("## Input: " + input);
        
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			// parse inputjson for jsonpath
			String intputstring = (String)input;//objectMapper.writeValueAsString(input);
			Object inputJson = Configuration.defaultConfiguration().jsonProvider().parse(intputstring);
	        // find manifest file key
			String manifestkey = JsonPath.read(inputJson, "$.manifestfile");
			log("## manifestkey: " + manifestkey);
	        
	        // find target file key(s)
			List<String> targetfilekeys = JsonPath.read(inputJson, "$.datafiles[*]");
			log("## targetkeys: " + targetfilekeys.size());
			
			// copy manifest
			log("## copying manifest file " + manifestkey);
			s3Client.copyObject(s3SourceBucket, manifestkey, s3TargetBucket, manifestkey);
			
			// copy data files
			for (String key : targetfilekeys) {
				log("## copying data file " + key);
				s3Client.copyObject(s3SourceBucket, key, s3TargetBucket, key);
			}
			
		} catch (AmazonServiceException e) {
			e.printStackTrace();
			logger.log("## Error copying files to S3");
			
		} catch (SdkClientException e) {
			e.printStackTrace();
			logger.log("## Error copying files to S3");
		} 

        // TODO: modify output to include more detailed information?
		// ie file count, discarded lines count etc...
        return "Modified files uploaded OK!";
    }

}
