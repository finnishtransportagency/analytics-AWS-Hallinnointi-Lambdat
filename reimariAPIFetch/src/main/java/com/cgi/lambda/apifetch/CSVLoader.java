package com.cgi.lambda.apifetch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;



public class CSVLoader implements Runnable{

	public CSVLoader(String clientRegion, String bucketName, String savePath,String url,String username, String password,Context context) {
	this.url=url;
	this.clientRegion=clientRegion;
	this.bucketName=bucketName;
	this.savePathAndFileName=savePath;
	this.username=username;
	this.password=password;
	this.context=context;	
	}
			
	private String username;
	private String password;
	private String csv;
	private String clientRegion;
	private String url;
	private String bucketName;
	private String savePathAndFileName;
	private Context context;

	
	
	
	/**
	 * fetches csv file and uploads it to S3
	 */
	@Override
	public void run() {
		try {
			csv= getCSV();
		} catch (IOException e1) {
			System.err.println("Fatal error: Failed to download csv" + savePathAndFileName);
			e1.printStackTrace();
			return ;
		}
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion(clientRegion).build();
		try {
			byte[] stringbytearray= csv.getBytes("UTF-8");
			InputStream byteString = new ByteArrayInputStream(stringbytearray);
			ObjectMetadata oMetadata = new ObjectMetadata();
			oMetadata.setContentType("plain/text");
			oMetadata.setContentLength(stringbytearray.length);
			s3Client.putObject(bucketName,savePathAndFileName, byteString,oMetadata);
			System.out.println("CSV load successfully");
			
		} catch (UnsupportedEncodingException e) {
			String errorMessage="Error: Failure to encode file to load in: " + savePathAndFileName;
			context.getLogger().log(errorMessage);
			
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		catch (Exception e) {
			
			String errorMessage="Error: S3 write error " + savePathAndFileName;
			context.getLogger().log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
	}

	public String getCSV() throws IOException {
    	String login = username + ":" + password;
    	String base64login = new String(Base64.encodeBase64(login.getBytes()));   
    	URL uurl = new URL(url);
    	URLConnection uc = uurl.openConnection();
    	uc.setRequestProperty("Authorization", "Basic " + base64login);		
    	   BufferedReader in   =   
    	            new BufferedReader (new InputStreamReader (uc.getInputStream(),"ISO-8859-1"));

    	   String line;
    	   StringBuilder csv = new StringBuilder();
    	   while ((line = in.readLine()) != null) {
    		   csv.append(line + "\n");
    	   }   
    	   in.close();
		return csv.toString();
	}

}
