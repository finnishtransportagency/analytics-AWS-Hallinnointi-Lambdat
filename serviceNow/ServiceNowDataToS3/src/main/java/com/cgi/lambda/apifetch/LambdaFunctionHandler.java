package com.cgi.lambda.apifetch;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;

import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {
	// Use environmental variables in AWS Lambda to set values of these
	private String username = System.getenv("username");
	private String password = System.getenv("password");
	private String url = System.getenv("service_url");
	private String slimit = System.getenv("splitlimit");
	// private String format=System.getenv("load_format"); // Only json for now.
	private String format = "JSONv2&displayvalue=true&sysparm_query=sys_updated_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()^ORsys_created_onONYesterday@javascript:gs.beginningOfYesterday()@javascript:gs.endOfYesterday()";
	private String region = System.getenv("region");
	private String s3Bucket = System.getenv("s3_bucket_name");
	private String alertString = System.getenv("alert_string");
	private String charset = System.getenv("charset");
	private Context context;
	private EnrichServiceNowDataWithCoordinates enrichmentCenter;
	// regex for accepted or expected characters in Service Now json. most notably alphanumeric chars incl
	// scandic letters, and chars used in json notation
	private static final String validchars = "[^a-zA-Z0-9åÅäÄöÖ\\ \\.\\,\\\"\\:\\(\\)\\[\\]\\{\\}\\-\\_\\/]";

	@Override
	public String handleRequest(Object input, Context context) {
		this.context = context;
		context.getLogger().log("Input: " + input);
		String data = "";
		String date = getDate(input);
		context.getLogger().log("date:  " + date + "\n");
		if (!date.isEmpty()) {
			context.getLogger().log("Notice: Date override to " + date);

			format = "JSONv2&displayvalue=true&sysparm_query=sys_created_onON" + date
					+ "@javascript:gs.dateGenerate(%27" + date + "%27,%27start%27)@javascript:gs.dateGenerate(%27"
					+ date + "%27,%27end%27)^ORsys_updated_onON" + date + "@javascript:gs.dateGenerate(%27" + date
					+ "%27,%27start%27)@javascript:gs.dateGenerate(%27" + date + "%27,%27end%27)";
			context.getLogger().log("Notice: URL:  " + url + format);
		}
		String newJsonString = "";
		// Get data to be saved to S3
		try {
			data = getData();
			// TODO: check data for invalid characters
			validate(data);
		} catch (IOException e) {
			System.err.println("Fatal error: Failed to download data");
			e.printStackTrace();
		}

		// Save data into S3

		if (!data.isEmpty()) {
			// TODO: In future, filename "u_case" should not probably be fixed (or atleast
			// hardcoded), since other ServiceNow
			// tables may be fetched (u_case is just a test).

			try {
				enrichmentCenter = new EnrichServiceNowDataWithCoordinates(context, data, "EPSG:3067",
						Integer.parseInt(slimit)); // 3067 =ETRS89 / ETRS-TM35FIN, converts to 4326=wgs84
				newJsonString = enrichmentCenter.enrichData();
			} catch (Exception e) {
				context.getLogger().log("Error: Could not add WGS84 coordinates to data or split limit not integer");
				context.getLogger().log("Error: WGS data: " + data.length());
				StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
				String sStackTrace = sw.toString(); // stack trace as a string
				context.getLogger().log("Error: : " + sStackTrace);

			}

			// adds wgs84 coordinates
			if (enrichmentCenter == null || enrichmentCenter.EnrichedList.isEmpty()) {
				context.getLogger().log("Error: Empty dataset");
				return "enrichedList";
			} else if (enrichmentCenter.EnrichedList.size() == 1) {
				long now = Instant.now().toEpochMilli();
				context.getLogger().log("single array found");
				saveData(newJsonString,
						new SimpleDateFormat("dd.MM.yyyy").format(new Date()) + "/" + now + "/" + "u_case.json");
			} else { // loop through array
				context.getLogger().log("multiarray found");
				int size = enrichmentCenter.EnrichedList.size();
				for (int i = 0; i < size; i++) {
					long now = Instant.now().toEpochMilli();
					saveData(enrichmentCenter.EnrichedList.get(i).toString(),
							(new SimpleDateFormat("dd.MM.yyyy").format(new Date()) + "/" + now + "/" + "u_case.json"));
				}
				context.getLogger().log("multiarray search ended");
			}
		}
		return "";
	}

	/**
	 * 1. Tarkistaa, etta merkijono on jsonia
	 * 2. Tarkistaa  etta sisaltaa vain sallittuja merkkeja
	 * 
	 * Datan latauksessa {@link #getData()} on jo koitettu tunnistaa alkuperainen merkisto
	 * 
	 * @param data, json merkkijono
	 */
	private void validate(String data) {
		// 1. Onko json
		// todo: tuplatarkastus, eli json muunnokset myohemmassa vaiheessa ajavat saman asian, voi mahd. poistaa
		// riippuen siita, halutaanko virhe havaita jo aiemmin ja keskitetysti
		try {
			new JSONObject(data);
		} catch (JSONException e) {
			try {
				new JSONArray(data);
			} catch (JSONException e2) {
				System.out.println("## Service now tiedosto ei ollut JSON muotoa");
				context.getLogger().log(alertString + "Service now tiedosto ei ollut JSON muotoa");
			}
		}
		
		// 2. Onko sisalto ok
		try {
			Pattern pattern = Pattern.compile(validchars);
			Matcher matcher = pattern.matcher(data);
			if(matcher.find()) {
				String invalidchars = matcher.group();
				context.getLogger().log(alertString + "JSON tiedostosta lyotyi odottamaton merkkijono: " + invalidchars);
			}
		} catch (Exception e) {
			context.getLogger().log("## Virhe regex tarkistuksessa: " + e.getMessage());
		}

	}

	protected String getDate(Object input) {
		String inputJson = input.toString().trim();
		if (inputJson.contains("date")) {
			return inputJson.substring(inputJson.indexOf("date") + 5, inputJson.length() - 1);
		} else {
			return "";
		}

	}

	public String getData() throws IOException {
		// merkiston tunnistusta varten
		CharsetDetector charsetDetector = new CharsetDetector();
		// Prepare login credentials and URL
		String login = username + ":" + password;
		String requestUrl = url + "?" + format;
		String base64Login = new String(Base64.getEncoder().encode(login.getBytes()));

		// Open connection and read CSV
		URL uurl = new URL(requestUrl);
		URLConnection uc = uurl.openConnection();
		uc.setRequestProperty("Authorization", "Basic " + base64Login);
		
		InputStream inputstream = uc.getInputStream();
		
		// koitetaan tunnistaa merkisto ja varmistaa, etta se vastaa sovittua
		try {
			context.getLogger().log("## Odotettu merkisto: " + charset);
			charsetDetector.setDeclaredEncoding(charset);
			charsetDetector.setText(inputstream);
			CharsetMatch match = charsetDetector.detect();
			context.getLogger().log("## Tunnistettu merkisto " + match.getName());
			context.getLogger().log("Merkisto tunnistettu varmuudella: " + match.getConfidence());
			if(match == null || !match.getName().equalsIgnoreCase(charset)) {
				StringBuffer sb = new StringBuffer(alertString);
				sb.append("Merkisto ei vastaa odotettua | ");
				sb.append("Odotettu: " + charset + " | ");
				sb.append("Tunnistettu: " + match.getName() + " | ");
				sb.append("Varmuus: " + match.getConfidence());
				context.getLogger().log(sb.toString());
			}
		} catch (Exception e) {
			context.getLogger().log("## Virhe merkiston tunnistuksessa: " + e.getMessage());
			context.getLogger().log(alertString + "Merkistoa ei tunnistettu tai syote oli tyhja.");
		}

		// Start reading
		BufferedReader in = new BufferedReader(new InputStreamReader(inputstream, "UTF-8"));

		String line;
		StringBuilder csv = new StringBuilder();
		while ((line = in.readLine()) != null) {
			csv.append(line + "\n");
		}
		in.close();
		return csv.toString();
	}

	public void saveData(String data, String path) {
		try {
			AmazonS3 s3Client = AmazonS3Client.builder().withRegion(region).build();
			byte[] stringByteArray = data.getBytes("UTF-8");
			InputStream byteString = new ByteArrayInputStream(stringByteArray);
			ObjectMetadata objMetadata = new ObjectMetadata();
			objMetadata.setContentType("plain/text");
			objMetadata.setContentLength(stringByteArray.length);

			s3Client.putObject(s3Bucket, path, byteString, objMetadata);
			System.out.println("Data loaded to S3 succesfully.");

		} catch (UnsupportedEncodingException e) {
			String errorMessage = "Error: Failure to encode file to load in: " + path;
			context.getLogger().log(errorMessage);

			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			String errorMessage = "Error: S3 write error " + path;
			context.getLogger().log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
	}

}
