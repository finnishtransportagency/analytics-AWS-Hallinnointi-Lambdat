package com.vayla.lambda.velho.dataloader;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.github.wnameless.json.flattener.JsonFlattener;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.vayla.lambda.velho.dataloader.ade.AdeManifestHelper;


public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	static final String debug = System.getenv("debug");
	static final String zip_encoding = "gzip";
	static final String csv_content_type = "text/csv";
	static final String manifest_content_type = "application/json";
	//static final String velhoS3Bucket = System.getenv("s3bucket");
	//static final String velhoS3Key = System.getenv("s3key");
	static final String workBucket = System.getenv("workbucket");
	static final String adeBucket = System.getenv("adebucket");
	static final String dataBucket = System.getenv("databucket");
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
    	if(!StringUtils.isEmpty(debug) && debug.equalsIgnoreCase("true")) log(s);
    }

    /**
     * Lukee joko eventista tai ymparistomuuttujan mukaisesta bucketista 
     * velho kaiteet.json ndjson tiedoston, muuttaa sen csv:ksi
     * ja tallentaa aden buckettiin manifestin kera
     */
    @Override
    public String handleRequest(S3Event event, Context context) {
    	this.logger = context.getLogger();
        log("## Received event: " + event);
        
        S3EventNotificationRecord record = event.getRecords().get(0);
        String srcBucket = record.getS3().getBucket().getName();
        // Object key may have spaces or unicode non-ASCII characters.
        String srcKey = record.getS3().getObject().getUrlDecodedKey();
        

        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.serializeNulls();
        Gson gson = gsonBuilder.create();
        ObjectMapper mapper = new ObjectMapper();
        
        try {
        	List<String> rivit = readNdJsonObject(srcBucket, srcKey);
	        debug("## data ready, start flattening " + System.currentTimeMillis());
	        
	        // litistetaan velho json csv:ta varten
	        List<Map<String, Object>> flatjson = new ArrayList<Map<String,Object>>();
	        for(String rivi : rivit) {
	        	debug(rivi);
	        	flatjson.add(new JsonFlattener(rivi).withSeparator('_').flattenAsMap());
	        }
	        log("## litistettyja kaiteita" + flatjson.size());
	        debug("## csv building started " + System.currentTimeMillis());
	        
	        // TODO: irroitetaan csv kirjoitin omaksi luokaksi tai funktioksi
	        // luodaan csv schema perustuen litistettyyn jsoniin ja kenttanimiin
	        Builder csvSchemaBuilder = CsvSchema.builder();
	        JsonNode jsonNode = mapper.readTree(gson.toJson(flatjson));
	        JsonNode firstObject = jsonNode.elements().next();
	        List<String> headers = new ArrayList<String>();
			firstObject.fieldNames().forEachRemaining(fieldName -> {
				csvSchemaBuilder.addColumn(fieldName);
				headers.add(fieldName);
			});
			// withHeader jatetaan pois, kun manifest ym valmiit ADE:a varten
			CsvSchema csvSchema = csvSchemaBuilder
					.build()
					.withLineSeparator("\r\n")
					.withoutHeader(); //.withHeader();
			
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			
			// kirjoitetaan csv muistiin
			CsvMapper csvMapper = new CsvMapper();
			csvMapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true); //ADE needs enclosing quotes for values?
			csvMapper.writerFor(JsonNode.class)
			  .with(csvSchema)
			  .writeValue(out,jsonNode);
			
			debug("## csv written to bytes " + System.currentTimeMillis());
			
			// tallennetaan csv s3 work buckettiin samaan "kansioon" ts. prefixilla kuin
			// alkuperainen zipatuksi csv:ksi
			// velhosta haettu data lotyy ns. landing bucketista ja key on muotoa data/ddmmyy(tai viimeisi)/varustetiedot/
			
			//String key = srcKey + ".csv.gz";
			
			// TODO: jatkokehityksena miten sailyttaa yleiskaytettavyys 
			// velhosta tulevien datatiedostojen tunnistamisessa ja nimeamisessa ADE-muotoon
			String csvfilekey = "manifest/velho_kaide/table.velho_kaide." + System.currentTimeMillis() + ".csv.gz";
			// Zip it
    		byte[] zippedData = GzipString.compress(out.toByteArray());
    		// save it
    		ObjectMetadata csvMetadata = new ObjectMetadata();
    		csvMetadata.setContentType(csv_content_type);
    		csvMetadata.setContentLength(zippedData.length);
    		csvMetadata.setContentEncoding(zip_encoding);
    		
    		// kohteeksi ade loader bucket
    		debug("## save object " + System.currentTimeMillis());
			saveObject(dataBucket, csvfilekey, zippedData, csvMetadata);
			debug("## object saved " + System.currentTimeMillis());
			
			// manifestin luonti ja tallennus
			URL s3url = s3.getUrl(adeBucket, csvfilekey);
			String fulls3name = getS3KeyFromURL(s3url);
			String prefix = FilenameUtils.getPath(fulls3name);
			String filename = FilenameUtils.getBaseName(fulls3name);
			String manifestFilename = "manifest-"+filename+".gz.json";
			String fullManifestFilename = prefix + manifestFilename;
			log("## manifest filename " + fullManifestFilename);
			
			String csvurl = "s3://" + dataBucket + "/" + csvfilekey;
			log("## csvurl " + csvurl);
			
			List<String> urls = new ArrayList<String>();
			urls.add(csvurl);
			String manifestjson = AdeManifestHelper.createManifest(urls, headers);
			log("## " + manifestjson);
			byte[] manifestdata = manifestjson.getBytes();
			ObjectMetadata manifestMetadata = new ObjectMetadata();
    		manifestMetadata.setContentLength(manifestdata.length);
    		manifestMetadata.setContentType(manifest_content_type);
			saveObject(dataBucket, fullManifestFilename, manifestdata, manifestMetadata);
            
        } catch (Exception e) {
            e.printStackTrace();
            context.getLogger().log(String.format(
                "Error getting object %s from bucket %s. Make sure they exist and"
                + " your bucket is in the same region as this function.", srcKey,srcBucket));
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
    
    private List<String> readNdJsonObject(String bucket, String key) {
    	List<String> ndJsonRivit = new ArrayList<String>();
    	String line;
    	S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
    	String encoding = response.getObjectMetadata().getContentEncoding();
        InputStream is = response.getObjectContent();
        
    	try {
    		if(encoding.equals(zip_encoding)) is = new GZIPInputStream(is);
        	BufferedReader rd = new BufferedReader(new InputStreamReader(is));
        	System.out.println(System.currentTimeMillis());
        	
	    	rd.readLine(); // hypataan metatieto rivi
	    	
	        while ((line = rd.readLine()) != null) {
	        	ndJsonRivit.add(line);
	        }
	        rd.close();
    	}catch(Exception e) {
    		e.printStackTrace();
    		logger.log("## Error reading file from S3 ");
    	}
	        
        log("## ndjsonrivit " + ndJsonRivit.size());
        debug("## " + System.currentTimeMillis());
        
        return ndJsonRivit;
    	
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