package com.vayla.lambda.velho.metadata;

import java.util.List;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vayla.lambda.velho.metadata.auth.AWS4SignerBase;
import com.vayla.lambda.velho.metadata.auth.AWS4SignerForAuthorizationHeader;
import com.vayla.lambda.velho.metadata.utils.HttpUtils;
import com.vayla.lambda.velho.metadata.utils.SecretManagerUtil;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_CENTRAL_1).build();
	static final String velhoHost = System.getenv("velhoHost"); //"api.stg.velho.vayla.fi";
	static final String velhoService = "execute-api"; // AWS API Gateway
	static final String velhoWorkBucket = System.getenv("workBucket");
	static final String velhoWorkBucketPrefix = System.getenv("metadataprefix");
	static final String landingBucket = System.getenv("landingbucket");
	LambdaLogger logger;

	public LambdaFunctionHandler() {
	}

	// Test purpose only.
	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	void log(String s) {
		if(logger != null) logger.log(s);
		else System.out.println(s);
	}

	@Override
	public String handleRequest(Object event, Context context) {
		//this.logger = context.getLogger();
		log("## Received event: " + event);
		
		ObjectMapper mapper = new ObjectMapper();
		
		//TODO: tuleeko metadatakutsut eventissa, vai listataanko valmiiksi ja kaydaan lapi?
		List<URL> urlList = new ArrayList<URL>();

		try {
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/kohdeluokat"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeistot"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/varustetiedot/kaidetyyppi"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/varustetiedot/materiaali"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/varustetiedot/kaidepylvastyyppi"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/varustetiedot/kaidenollaamatta"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/varustetiedot/lisatehtava"));
			urlList.add(new URL("https://api.stg.velho.vayla.fi/v1/nimikkeisto/yleiset/jarjestelmakokonaisuus"));
			
			for(URL myUrl : urlList) {
				// 1: luo headerit ja allekirjoitusavain 
				AWS4SignerForAuthorizationHeader signer = new AWS4SignerForAuthorizationHeader(myUrl, "GET", velhoService, "eu-central-1");
				Map<String, String> headers = new HashMap<String, String>();
				Map<String, String> queryParams = new HashMap<String, String>();
				
				String secrets = SecretManagerUtil.getSecret("VelhoSecrets", "eu-central-1");
				log("## got secrets: \n" + (secrets == null ? null : secrets.length()) ); // ei tulosteta avaimia edes aws lokiin
				
				if(secrets==null) throw new IllegalArgumentException("Salausavaimia ei kaytettavissa");
				JsonNode json = mapper.readTree(secrets);
				JsonNode velhoAccessKey = json.findValue("velhoAccessKey");
				JsonNode velhoSecretKey = json.findValue("velhoSecretKey");

				if(velhoAccessKey == null || velhoSecretKey == null) throw new IllegalArgumentException("Salausavain ei voi olla null");
				
				String authHeader = signer.computeSignature(headers, queryParams, AWS4SignerBase.EMPTY_BODY_SHA256, velhoAccessKey.asText(), velhoSecretKey.asText());
				log(authHeader);
				headers.put("Authorization", authHeader);
				
				// 2: kutsu velhon rajapintaa jokaiselle metadata jsonille
				String jsonString = HttpUtils.invokeHttpRequest(myUrl, "GET", headers, null);
				log("---------- RESPONSE ----------");
				log(jsonString);
				log("------------------------------");
				
				// 3: tallenna buckettiin
				String key = getS3KeyFromURL(myUrl);
				log("## recieved key");
				log(key);
				log("## key end");
				//add metadata prefix
				saveJSONToS3(velhoWorkBucketPrefix + key, jsonString);
			}

		} catch (Exception e) {
			e.printStackTrace();
			log("Virhe");
			System.exit(-1);
		}
		
		return "OK";
	}
	
	static String getS3KeyFromURL(URL endpoint) {
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
        
		return path+".json";
	}
	
	void saveJSONToS3(String key, String json) {
		log("************************************************");
        log("*        Executing       'saveJSONToS3'        *");
        log("************************************************");
                
        s3.putObject(landingBucket, key, json);
        
	}
}