package com.vayla.lambda.velho.metadata;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.amazonaws.util.SdkHttpUtils;

public class VelhoRequestSigner {
	
	static final String accessKey = System.getenv("velhoAccessKey");
	static final String secretKey = System.getenv("velhoSecretKey");
	
	static void log(String s){
		System.out.println(s);
	}
	
	public VelhoRequestSigner() {
		// TODO Auto-generated constructor stub
	}
	
	static byte[] HmacSHA256(String data, byte[] key) throws Exception {
	    String algorithm="HmacSHA256";
	    Mac mac = Mac.getInstance(algorithm);
	    mac.init(new SecretKeySpec(key, algorithm));
	    return mac.doFinal(data.getBytes("UTF-8"));
	}

	static byte[] getSignatureKey(String key, String dateStamp, String regionName, String serviceName) throws Exception {
	    byte[] kSecret = ("AWS4" + key).getBytes("UTF-8");
	    byte[] kDate = HmacSHA256(dateStamp, kSecret);
	    byte[] kRegion = HmacSHA256(regionName, kDate);
	    byte[] kService = HmacSHA256(serviceName, kRegion);
	    byte[] kSigning = HmacSHA256("aws4_request", kService);
	    return kSigning;
	} 
	
	private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();
	public static String bytesToHex(byte[] bytes) {
	    char[] hexChars = new char[bytes.length * 2];
	    for (int j = 0; j < bytes.length; j++) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
	        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	static String getVelhoAuthKey(String method, String service, String host, String region, String uri, String requestParameters, String amzdate, String datestamp) throws Exception {
		
		/*
		 *  TASK 1: CREATE A CANONICAL REQUEST
		 */
		// Step 1 is to define the verb (GET, POST, etc.)--already done.

		// Step 2: Create canonical URI--the part of the URI from domain to query 
		// Normalize URI paths according to RFC 3986. Remove redundant and relative path components. 
		// Each path segment must be URI-encoded twice
		// string (use '/' if no path)
		String canonicalUri = SdkHttpUtils.urlEncode(uri, true);//"/";
		

		// Step 3: Create the canonical query string. In this example (a GET request),
		// request parameters are in the query string. Query string values must
		// be URL-encoded (space=%20). The parameters must be sorted by name.
		// For this example, the query string is pre-formatted in the request_parameters variable.
		String canonicalQuerystring = requestParameters;

		// Step 4: Create the canonical headers and signed headers. Header names
		// must be trimmed and lowercase, and sorted in code point order from
		// low to high. Note that there is a trailing \n.
		String canonicalHeaders = "host:" + host + "\n" + "x-amz-date:" + amzdate + "\n";

		// Step 5: Create the list of signed headers. This lists the headers
		// in the canonical_headers list, delimited with ";" and in alpha order.
		// Note: The request can include any headers; canonical_headers and
		// signed_headers lists those that you want to be included in the 
		// hash of the request. "Host" and "x-amz-date" are always required.
		String signedHeaders = "host;x-amz-date";

		// Step 6: Create payload hash (hash of the request body content). For GET
		// requests, the payload is an empty string ("").
		MessageDigest digest = MessageDigest.getInstance("SHA-256");
		digest.update("".getBytes(StandardCharsets.UTF_8));
		byte[] hash = digest.digest();
		String payloadHash = bytesToHex(hash).toLowerCase();

		// Step 7: Combine elements to create canonical request
		String canonicalRequest = method + "\n" + canonicalUri + "\n" + canonicalQuerystring + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash;
		log("canonical request: \n" + canonicalRequest);
		
		/*
		 * TASK 2: CREATE THE STRING TO SIGN
		 */
		String algorithm = "AWS4-HMAC-SHA256";
		String credentialScope = datestamp + "/" + region + "/" + service + "/" + "aws4_request";
		digest.reset();
		digest.update(canonicalRequest.getBytes(StandardCharsets.UTF_8));
		byte[] hash2 = digest.digest();
		String requestHash = bytesToHex(hash2).toLowerCase();
		
		// Combine
		String stringToSign = algorithm + "\n" + amzdate + "\n" + credentialScope + "\n" + requestHash;
		log("string to sign: \n" + stringToSign);
		
		/**
		 * TASK 3: CALCULATE THE SIGNATURE
		 */
		
		// Step 1. Create the signing key using the function defined above.
		byte[] signingKey = getSignatureKey(secretKey, datestamp, region, service);
		
		// Step 2. Sign the stringToSign using the signingKey
		// TODO:signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
		String signature = bytesToHex(HmacSHA256(stringToSign, signingKey)).toLowerCase();
		log("signature: " + signature);
		
		/**
		 * TASK 4: 	RETURN SIGNING INFORMATION TO THE REQUEST
		 */
		String authorizationHeader = algorithm + " " + "Credential=" + accessKey + "/" + credentialScope + ", " +  "SignedHeaders=" + signedHeaders + ", " + "Signature=" + signature;
		
		return authorizationHeader;
	}

}
