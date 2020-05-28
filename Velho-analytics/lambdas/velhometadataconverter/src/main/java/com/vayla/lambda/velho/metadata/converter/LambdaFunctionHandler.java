package com.vayla.lambda.velho.metadata.converter;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.StringUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vayla.lambda.velho.metadata.converter.ade.AdeManifestHelper;
import com.vayla.lambda.velho.metadata.converter.ade.Nimike;
import com.vayla.lambda.velho.metadata.converter.ade.NimikeCsvHelper;
import com.vayla.lambda.velho.metadata.converter.ade.NimikeCsvHelper.HEADER;
import com.vayla.lambda.velho.metadata.converter.util.GzipString;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {
	static final String VERSION_IN_USE = System.getenv("schema_versio"); //esim "1";
	static final String DEBUG = System.getenv("debug");
	static final String ZIP_ENCODING = "gzip";
	static final String CSV_CONTENT_TYPE = "text/csv";
	static final String MANIFEST_CONTENT_TYPE = "application/json";
	static final String WORKBUCKET = System.getenv("workbucket");
	static final String ADEBUCKET = System.getenv("adeBucket");
	static Map<String, String> MANIFESTMAP;
	
	static {
		MANIFESTMAP = new HashMap<String, String>();
		MANIFESTMAP.put("materiaali", "velho_kaide_materiaali");
		MANIFESTMAP.put("kaidetyyppi", "velho_kaidetyyppi");
		MANIFESTMAP.put("kaidepylvastyyppi", "velho_kaidepylvastyyppi");
		MANIFESTMAP.put("kaidenollaamatta", "velho_kaidenollaamatta");
		MANIFESTMAP.put("lisatehtava", "velho_kaidelisatehtava");
		MANIFESTMAP.put("jarjestelmakokonaisuus", "velho_yleinen_jarjestelmakokonaisuus");
	}

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1).build();
    LambdaLogger logger;

    public LambdaFunctionHandler() {}

    // Test purpose only.
    LambdaFunctionHandler(AmazonS3 s3) {
        this.s3 = s3;
    }
    
    void log(String s) {
    	logger.log(s);
    }
    
    void debug(String s) {
    	if(!StringUtils.isNullOrEmpty(DEBUG) && DEBUG.equalsIgnoreCase("true")) log(s);
    }

    @Override
    public String handleRequest(S3Event event, Context context) {
    	this.logger = context.getLogger();
        log("## Received event: " + event);

        // Get the object from the event and show its content type
        String bucket = event.getRecords().get(0).getS3().getBucket().getName();
        String key = event.getRecords().get(0).getS3().getObject().getKey();
        try {
        	String metadataJson = readObject(bucket, key);
        	
        	String metadataName = FilenameUtils.getBaseName(key);
        	log("## metadata file name: " + metadataName);
        	String metadataAdeName = MANIFESTMAP.get(metadataName);
        	log("## metadata ade name: " + metadataAdeName);
		        
			ObjectMapper mapper = new ObjectMapper();

			JsonNode rootNode = mapper.readTree(metadataJson);
			// TODO: tarkista "uusin-nimikkeistoversio" arvo ja hälytä, jos eri, kuin VERSION_IN_USE
			JsonNode nimikkeistoNode = rootNode.path("nimikkeistoversiot").path(VERSION_IN_USE);
			
			List<Nimike> nimikkeisto = new ArrayList<Nimike>();
			nimikkeistoNode.fields().forEachRemaining(nimike -> {
				debug("## "+nimike);
				
				try {
					Nimike n = mapper.treeToValue(nimike.getValue(), Nimike.class);
					String koodi = nimike.getKey();
					n.setKoodi(koodi);

					int loc = koodi.indexOf("/");
					String luokka = nimike.getKey().substring(0, loc > -1 ? loc : koodi.length()-1);
					n.setNimi(luokka);
					nimikkeisto.add(n);
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
			
			StringBuilder csvString = new StringBuilder();
			for(Nimike n : nimikkeisto) {		
				String s = NimikeCsvHelper.getNimikeAsCsv(n) + "\r\n";
				csvString.append(s);
			}
			
			byte[] zippedData = GzipString.compress(csvString.toString().getBytes());
			
			log("## csv written to file ");		
			debug("## "+System.currentTimeMillis());
			
			// save it
    		ObjectMetadata csvMetadata = new ObjectMetadata();
    		csvMetadata.setContentType(CSV_CONTENT_TYPE);
    		csvMetadata.setContentLength(zippedData.length);
    		csvMetadata.setContentEncoding(ZIP_ENCODING);
    		
			//String csvfilekey = "manifest/" + "velho_kaide_materiaali" + "/table." + "velho_kaide_materiaali" + "." + System.currentTimeMillis() + ".csv.gz";
    		String csvfilekey = "manifest/" + metadataAdeName + "/table." + metadataAdeName + "." + System.currentTimeMillis() + ".csv.gz";
    		
    		// kohteeksi ade loader bucket
    		debug("## save object " + System.currentTimeMillis());
			saveObject(ADEBUCKET, csvfilekey, zippedData, csvMetadata);
			debug("## object saved " + System.currentTimeMillis());
			
			// manifestin luonti ja tallennus
			URL s3url = s3.getUrl(ADEBUCKET, csvfilekey);
			String fulls3name = getS3KeyFromURL(s3url);
			String prefix = FilenameUtils.getPath(fulls3name);
			String filename = FilenameUtils.getBaseName(fulls3name);
			String manifestFilename = "manifest-"+filename+".gz.json";
			String fullManifestFilename = prefix + manifestFilename;
			log("## manifest filename " + fullManifestFilename);
			
			String csvurl = "s3://" + ADEBUCKET + "/" + csvfilekey;
			log("## csvurl " + csvurl);
			
			List<String> urls = new ArrayList<String>();
			urls.add(csvurl);
			List<String> headers = new ArrayList<String>();
			for(HEADER h : NimikeCsvHelper.HEADER.values()) {
				headers.add(h.toString());
			}
			
			String manifestjson = AdeManifestHelper.createManifest(urls, headers);
			log("## " + manifestjson);
			byte[] manifestdata = manifestjson.getBytes();
			ObjectMetadata manifestMetadata = new ObjectMetadata();
    		manifestMetadata.setContentLength(manifestdata.length);
    		manifestMetadata.setContentType(MANIFEST_CONTENT_TYPE);
			saveObject(ADEBUCKET, fullManifestFilename, manifestdata, manifestMetadata);

        } catch (Exception e) {
            e.printStackTrace();
            log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", key, bucket));
            throw new RuntimeException("Tapahtui virhe json - csv muunnoksessa", e);
        }
        
        return "OK";
    }
    
 // tallennus
    private void saveObject(String bucket, String key, byte[] data, ObjectMetadata objectMetadata) {
    	try {
    		log("## Saving an object to: " + bucket + " " + key);
    		
    		// Save it
    		ByteArrayInputStream byteIn = new ByteArrayInputStream(data);
    		s3.putObject(bucket, key, byteIn, objectMetadata);
    	} catch(SdkClientException e) {
    		e.printStackTrace();
    		logger.log("## Error saving file to S3 " + bucket + ", " + key);
    	} 
    	
    }
    
    // tiedoston luku
    private String readObject(String bucket, String key) {
    	StringBuilder sb = new StringBuilder();
		String line;
		
    	try {
    		S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
            String contentType = response.getObjectMetadata().getContentType();
            log("## CONTENT TYPE: " + contentType);
            
            InputStream is = response.getObjectContent();
			BufferedReader rd = new BufferedReader(new InputStreamReader(is));
			
			while ((line = rd.readLine()) != null) {
				sb.append(line);
			}
			rd.close();
			log("## luetut rivit " + sb.length());    		
    	} catch(SdkClientException e) {
    		e.printStackTrace();
    		logger.log("## Error reading file from S3 " + bucket + ", " + key);
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return sb.toString();
    	
    }
    
    private static String getS3KeyFromURL(URL endpoint) {
		if ( endpoint == null ) {
            throw new IllegalArgumentException("URL can't be null");
        }
        String path = endpoint.getPath();
        if ( path == null || path.isEmpty() ) {
            throw new IllegalArgumentException("URL must have a path");
        }
        // drop the leading separator for s3 key
        if (path.startsWith("/")) {
        	path = path.substring(1);
        }
        
		return path;
	}
}