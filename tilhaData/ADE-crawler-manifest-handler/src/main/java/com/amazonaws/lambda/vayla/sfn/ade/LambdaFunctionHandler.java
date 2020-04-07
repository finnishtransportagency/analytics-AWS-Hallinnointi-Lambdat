package com.amazonaws.lambda.vayla.sfn.ade;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;

/**
 * Read given manifest file from the s3 event. Download target data JSON and save both files in
 * the work bucket.
 *
 */
public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	private static final boolean DEBUG = true;
	LambdaLogger logger = null;
	AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();
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
		log("## Input: " + input);

		ObjectMapper objectMapper = new ObjectMapper();

		try {
			String json = objectMapper.writeValueAsString(input);
			log("## json = " + json);

			Object eventJson = Configuration.defaultConfiguration().jsonProvider().parse(json);

			// AWS S3 put event, bucket name
			String eventS3Bucket = JsonPath.read(eventJson, "$.Records[0].s3.bucket.name");
			log("## S3 bucket: " + eventS3Bucket);

			// AWS S3 put event, key value
			String eventS3Key = JsonPath.read(eventJson, "$.Records[0].s3.object.key");
			log("## S3 key: " + eventS3Key);

			String jsonString = getObject(eventS3Bucket, eventS3Key);
			Object manifestJson = Configuration.defaultConfiguration().jsonProvider().parse(jsonString);

			// ADE crawler table manifest, data can be split to multiple files
			List<String> targetFileURLs = JsonPath.read(manifestJson, "$.entries[*].url");
			for (String url : targetFileURLs) {
				log("## target file URL: " + url);
			}

			// 1. copy manifest
			CopyObjectRequest copyObjRequest = new CopyObjectRequest(eventS3Bucket, eventS3Key, s3WorkBucket,
					eventS3Key);
			log("## Copying manifest to work bucket: " + eventS3Key);
			s3Client.copyObject(copyObjRequest);

			// 2. copy data files
			List<String> datafiles = new ArrayList<String>();
			for (String url : targetFileURLs) {
				URI uri = new URI(url);
				// key sis. s3 "kansiot", eli koko sijaintipolku bucketin perassa
				String s3key = uri.getPath();
				s3key = s3key.substring(1); // pudotetaan polun ensimmainen kenoviiva
				CopyObjectRequest copyObjRequest2 = new CopyObjectRequest(eventS3Bucket, s3key, s3WorkBucket, s3key);
				log("## Copying targetfile: " + s3key);
				s3Client.copyObject(copyObjRequest2);
				datafiles.add(s3key);
			}

			// 3. generate output with manifest and file info
			Fileinfo fileinfo = new Fileinfo();
			fileinfo.setManifestfile(eventS3Key);
			fileinfo.setDatafiles(datafiles);
			
			output = objectMapper.writeValueAsString(fileinfo);

		} catch (JsonProcessingException e) {
			logger.log("## Could not parse manifest json");
			logger.log(e.getMessage());
		} catch (URISyntaxException e) {
			logger.log("## Could not parse uri from manifest");
			logger.log(e.getMessage());
		} catch (AmazonServiceException e) {
			logger.log(e.getMessage());
		} catch (SdkClientException e) {
			logger.log(e.getMessage());
		}

		return output;
	}

	// Reads JSON file from given bucket and returns it as a string
	private String getObject(String eventS3Bucket, String eventS3Key) {
		StringBuffer sb = new StringBuffer();
		try {
			log("## Downloading an object");
			S3Object fullObject = s3Client.getObject(new GetObjectRequest(eventS3Bucket, eventS3Key));
			log("## Content-Type: " + fullObject.getObjectMetadata().getContentType());
			BufferedReader reader = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
			logger.log("## Error reading JSON from S3: " + eventS3Bucket + " ," + eventS3Key);
		}

		return sb.toString();

	}
	

}
