package com.cgi.lambda.exceltocsv;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.apache.commons.io.FilenameUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3Entity;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import org.joda.time.DateTime;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	// Parametrit, lambdalle voi antaa vain ne joita pitää muuttaa oletusarvoista
	private String delimiter = ";";
	private String eol = "\n";
	private String quote = "\"";
	private String quoteEscape = "\"";
	private String charset = "UTF-8";
	private String replaceCR = "";
	private String replaceNL = " ";
	private String fileType = "";
	private boolean trimData = true;
	private boolean hasHeader = true;
	private int skipheaders = 0;


	private Context context;

	private SimpleLambdaLogger logger = null;	

	private String runYearMonth = "";
	private boolean includeYearMonth = true;

	private String outputBucket = null;
	private String outputPath = null;

	private String archiveBucket = null;
	private String archivePath = null;

	private String listOfFiles = null;
	private boolean processOnlyListedFiles = true;

	private String listOfSheets = null;

	private Boolean fileHasTimestamp = false;
	private String timestampDelimiter = "_";

	private String removePrefix = "";
	private String prefix = "";
	private String sourceSystem = "";


	@Override
	public String handleRequest(S3Event event, Context context) {

		// Initialize Excel workbook factories for HSSF (xls) and XSSF (xlsx) formats
		WorkbookFactory.addProvider(new HSSFWorkbookFactory());
		WorkbookFactory.addProvider(new XSSFWorkbookFactory());

		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);



		// Käsiteltävät tiedostot
		this.listOfFiles = System.getenv("list_of_files");
		if (this.listOfFiles == null || this.listOfFiles == ""){
			this.logger.log("The lambda has not been set to process any files. Add filenames to list_of_files environment variable.");
			this.logger.log("Use semicolon followed by space to separate filenames and don't include file extension.");
			this.logger.log("Nothing to do here. Terminating lambda.");
			return "";
		}

		// Käsiteltävät sivut
		this.listOfSheets = System.getenv("list_of_sheets");
		if (this.listOfSheets == null || this.listOfSheets == ""){
			this.listOfSheets = "";
			this.logger.log("The lambda has not been set to process any specific sheets. All sheets will be included in csv.");
			this.logger.log("If you want to set sheetnames to process, add filenames to list_of_sheets environment variable.");
			this.logger.log("Use semicolon followed by space to separate sheetnames.");
		}

		// Vuosikuukausi
		this.runYearMonth = DateTime.now().toString("yyyy/MM/dd");
		String t = System.getenv("add_path_ym");
		if (t == null) t = "";
		if (t.length() > 0){
			if ("1".equals(t) || "true".equalsIgnoreCase(t) ) {
				this.includeYearMonth = true;
			}
			else if ("0".equals(t) || "false".equalsIgnoreCase(t) ) {
				this.includeYearMonth = false;
			}
		}


		// Kohde
		this.outputBucket = System.getenv("output_bucket");
		if (this.outputBucket == null) this.outputBucket = "";
		if (this.outputBucket.length() <= 0){
			this.logger.log("Kohde bucket puuttuu. Lopetetaan lambda...");
			return "";
		}

		this.outputPath = System.getenv("output_path");
		if (this.outputPath == null) this.outputPath = "";
		if (!this.outputPath.equals("")) {
			if (!this.outputPath.endsWith("/")){
				this.outputPath += "/";
			}
		}
		// Arkisto
		this.archiveBucket = System.getenv("archive_bucket");
		if (this.archiveBucket == null) this.archiveBucket = "";
		this.archivePath = System.getenv("archive_path");
		if (this.archivePath.length() > 0) {
			if (!this.archivePath.endsWith("/")) {
				this.archivePath += "/";
			}
		}

		// Käsitelläänkö vain listatut tiedostot?
		t = System.getenv("process_only_listed_files");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.processOnlyListedFiles = true;
				this.logger.log("Prosessoidaan vain listatut tiedostot");
			} else {
				this.processOnlyListedFiles = false;
				this.logger.log("Prosessoidaan kaikki tiedostot");
			}
		}

		// Onko tidostonimen lopussa aikaleima?
		t = System.getenv("timestamp_included");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.fileHasTimestamp = true;
			} else {
				this.fileHasTimestamp = false;
			}
		}

		// Aikaleiman erotin
		t = System.getenv("timestamp_delimiter");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.timestampDelimiter = t;
		} else {
			this.timestampDelimiter = "_";
		}

		// poistetaan nimen alusta
		t = System.getenv("remove_prefix");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.removePrefix = t;
		}

		// lisätään nimen alkuun table. jälkeen
		t = System.getenv("prefix");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.prefix = t;
		}

		// Lähde järjestelmä ADE:ssa
		t = System.getenv("ade_source_system");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.prefix = t;
		}

		// // lisätään polussa nimen alkuun
		// t = System.getenv("output_prefix");
		// if (t == null) t = "";
		// if (t.length() > 0) {
		// 	this.prefix = t;
		// }


		// Muuntimen parametrit
		t = System.getenv("charset");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.charset = t;
		}
		t = System.getenv("delimiter");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.delimiter = t;
		}
		t = System.getenv("eol");
		if (t == null) t = "";
		if (t.length() > 0) {
			if (t.equalsIgnoreCase("crlf") || t.equalsIgnoreCase("\\r\\n")) {
				this.eol = "\r\n";
			} else if (t.equalsIgnoreCase("cr") || t.equalsIgnoreCase("\\r")) {
				this.eol = "\r";
			} else if (t.equalsIgnoreCase("lf") || t.equalsIgnoreCase("\\n")) {
				this.eol = "\n";
			} else if (t.equalsIgnoreCase("lfcr") || t.equalsIgnoreCase("\\n\\r")) {
				this.eol = "\n\r";
			} else {
				this.eol = t;
			}

		}
		t = System.getenv("hasheader");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.hasHeader = true;
			} else {
				this.hasHeader = false;
			}
		}

		t = System.getenv("quote");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.quote = t;
		}
		t = System.getenv("quoteescape");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.quoteEscape = t;
		}

		t = System.getenv("replacecr");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.replaceCR = t;
		}

		t = System.getenv("replacenl");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.replaceNL = t;
		}

		t = System.getenv("skipheaders");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.skipheaders = Integer.valueOf(t).intValue();
		}

		t = System.getenv("trimdata");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.trimData = true;
			} else {
				this.trimData = false;
			}
		}

		S3Entity s3Entity = event.getRecords().get(0).getS3();
		String sourceBucket = s3Entity.getBucket().getName();
		String sourceKey = s3Entity.getObject().getKey();

		String[] sourceKeyPieces = sourceKey.split("/");
		String sourceFileName = sourceKeyPieces[sourceKeyPieces.length - 1].toLowerCase();

		String baseName = FilenameUtils.getBaseName(sourceFileName);

		this.logger.log("sourceFileName is: " + sourceFileName);

		

		if (!sourceFileName.endsWith(".xls") && !sourceFileName.endsWith(".xlsx")) {
			this.logger.log("Not an excel file (" + sourceBucket + sourceKey +  "). Nothing to do => Exit");
			return "";
		}
		else if (sourceFileName.endsWith(".xls")) {
			this.fileType = "xls";
		}
		else{
			this.fileType = "xlsx";
		}

		if (this.fileHasTimestamp){
			String sourceFileNameWithoutTimestamp = sourceFileName;

			// Odotetaan tiedostoa muodossa source_filename_time-stamp.xlsx
			// esim. hato_kehhanke_2023-01-04-14-07-05-197.xlsx

			try {
				int sepPos;
				sepPos = sourceFileName.lastIndexOf(this.timestampDelimiter);
				sourceFileNameWithoutTimestamp = sourceFileName.substring(0, sepPos);
				int timestampYear = 0;
				try{
					timestampYear = Integer.parseInt(sourceFileName.substring(sepPos+1, sepPos+5));
				}catch (NumberFormatException e){
					this.logger.log("Can't convert timestamp first 4 char to number");
				}
				
				if (timestampYear < 2200 && timestampYear > 2000){
					this.logger.log("Timestamp:  " + sourceFileName.substring(sepPos+1, sourceFileName.length()-this.fileType.length()));
					sourceFileNameWithoutTimestamp = sourceFileNameWithoutTimestamp + "." + this.fileType;
					this.logger.log("sourceFileNameWithoutTimestamp is: " + sourceFileNameWithoutTimestamp);

					baseName = FilenameUtils.getBaseName(sourceFileNameWithoutTimestamp);
				}
				else{
					this.logger.log(sourceFileName.substring(sepPos+1, sourceFileName.length()-this.fileType.length()) + " doesn't seem like a proper timestamp...");
					this.logger.log("Oletetaan että aikaleima puuttuu ja jatketaan ilman aikaleimmaa...");
					// sourceFileNameWithoutTimestamp = sourceFileName;
				}
			} catch (StringIndexOutOfBoundsException e) {
				this.logger.log("Missing ' "+ this.timestampDelimiter +" ' in: " + sourceFileName);
				this.logger.log("Timestamp missing.");
				// sourceFileNameWithoutTimestamp = sourceFileName;
			}
			
		}

		String logThisThing = ("Kasitellaan tiedosto: " + baseName + "." + this.fileType);

		// Käsitelläänkö vain listatut tiedostot
		if (this.processOnlyListedFiles){

			// Split the list of files to process
			String[] filesToProcess = this.listOfFiles.split("; ");
			boolean nameIsOnTheList = false;

			// Iterate through the list of files to check if the base name is on the list
			for (String s : filesToProcess){
				if (baseName.equals(s.toLowerCase())){
					this.logger.log("Nimi on kasiteltavien tiedostojen listalla.");
					nameIsOnTheList = true;
				}
			}

			// Log the processing if the name is on the list, otherwise exit
			if (nameIsOnTheList){
				this.logger.log(logThisThing);
			}
			else {
				this.logger.log("The file " + baseName.toUpperCase() + " is not on the list of files to process => Exit");
				return "";
			}
		}
		else{
			// Log a message when no specific files are defined for processing
			this.logger.log("Kasiteltavia tiedostoja ei ole erikseen maaritelty.");
			this.logger.log(logThisThing);
		}	


		if (sourceBucket.equals(this.archiveBucket) && sourceKey.startsWith(this.archivePath)) {
			this.logger.log("Do not process archive path => Exit");
			return "";
		}

		String sheet = "";

		S3Client s3Client = S3Client.builder().build();
		GetObjectRequest getRequest = GetObjectRequest.builder().bucket(sourceBucket).key(sourceKey).build();
		InputStream in = s3Client.getObject(getRequest);

		String sheetNames[] = null;

		if (sheet.length() > 0) {
			sheetNames = new String[1];
			sheetNames[0] = sheet;
		}

		// Split the list of sheets to process (list from environment variable)
		String[] sheetsToProcess = this.listOfSheets.split("; ");

		// Check if there are sheets to process from the environment variable
		if (sheetsToProcess.length>0){
			// If sheets are specified, use them for processing
			sheetNames = sheetsToProcess;
			this.logger.log("Sheets to process:");
			for (int i = 0; i < sheetNames.length; i++) {
				this.logger.log(sheetNames[i]);
			}
		}		
		
		// Muunnos
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			XlsToCsvConverter converter = new XlsToCsvConverter();
			converter.setCharSet(this.charset);
			converter.setDelimiter(this.delimiter);
			converter.setEOL(this.eol);
			converter.setHasHeader(this.hasHeader);
			converter.setQuote(this.quote);
			converter.setQuoteEscape(this.quoteEscape);
			converter.setReplaceCR(this.replaceCR);
			converter.setReplaceNL(this.replaceNL);
			converter.setSkipheaders(this.skipheaders);
			converter.setTrimData(this.trimData);
			converter.setSheetNames(sheetNames);
			converter.setLogger(this.logger);
			converter.convert(in, out, this.fileType/*, excelFile*/);
			in.close();

			// Csv- data
			String data = out.toString(this.charset);
			out.close();

			// Kohteen kirjoitus
			//FileSpec outputFile = makeDataFileName(targetName);
			FileSpec outputFile = makeDataFileName(baseName);
			this.writeDataFile(s3Client, outputFile, data);

			// Lähteen arkistointi, arkistopolkuun lisätään päivämäärä (yyyyMMDD) ja tiedoston nimen eteen siirtoaikaleima (yyyyMMddhhmmss)
			if (this.archiveBucket.length() > 0) {
				DateTime dateTimeOfOutputFile = new DateTime(Long.parseLong(outputFile.timestamp));
				String today = dateTimeOfOutputFile.toString("yyyyMMdd");
				this.moveSourceFile(s3Client, sourceBucket, sourceKey, this.archiveBucket, this.archivePath + today + "/" + outputFile.timestamp + " " + sourceFileName);
			}

		} catch (Exception e) {
			this.logger.log("Conversion exception: '" + e.toString() + "', '" + e.getMessage() + "'");
		}
		
		return "";
	}

	
	

	public boolean moveSourceFile(S3Client s3Client, String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
		String sourceFullPath = sourceBucket + "/" + sourceKey;
		String targetFullPath = targetBucket + "/" + targetKey;
		this.logger.log("Move '" + sourceFullPath + "' -> '" + targetFullPath + "'");
		try {
			CopyObjectRequest copyRequest = CopyObjectRequest.builder()
				.sourceBucket(sourceBucket)
				.sourceKey(sourceKey)
				.destinationBucket(targetBucket)
				.destinationKey(targetKey)
				.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
				.build();
			s3Client.copyObject(copyRequest);
			DeleteObjectRequest delRequest = DeleteObjectRequest.builder()
				.bucket(sourceBucket)
				.key(sourceKey).build();
			s3Client.deleteObject(delRequest);
			
		} catch (Exception e) {
			this.logger.log("Move '" + sourceFullPath + "' -> '" + targetFullPath + "' failed: '" + e.toString() + "', '" + e.getMessage() + "'");
			return(false);
		}
		return(true);
	}



	

	/**
	 * Kirjoitettavan tiedoston nimen muodostus
	 * 
	 * <output prefix> / <today: dd.MM.yyyy> / <aikaleima: unix timestamp> / <tiedoston nimi>
	 * 
	 * @author Isto Saarinen 2021-12-02
	 * 
	 * @return tiedosto ja polku
	 */
	// s3://<outputBucket>/<outputPath>/[YYYY-MM/]table.<outputFileName>.<now>.fullscanned.true.delim.semicolon.skiph.1.csv
	public FileSpec makeDataFileName(String name) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;

		if (retval.path.length() > 0){
			retval.sourceSystem = getLastPart(retval.path);
		}

		// if (retval.path.length() > 0){
		// 	retval.sourceSystem = this.outputPath.replaceAll("/", "_");
		// 	if (!retval.sourceSystem.endsWith("_")){
		// 		retval.sourceSystem += "_";
		// 	}
		// }

		

		if (name.startsWith(retval.sourceSystem)) {
			retval.sourceName = name.replaceFirst((retval.sourceSystem + "_"), "");
		}
		else{
			retval.sourceName =  name;
		}
		retval.path += retval.sourceName + "/";
		if (this.includeYearMonth) {
			if (!retval.path.endsWith("/")) retval.path += "/";
			retval.path += this.runYearMonth + "/";
		}
		retval.timestamp = "" + DateTime.now().getMillis();
		
		retval.fileName = "table." + retval.sourceSystem + "_" + retval.sourceName + "." + retval.timestamp + ".fullscanned." + retval.fullscanned + ".delim."+ retval.delimiter +".skiph."+ retval.skipHeaders +".csv";
		return retval; 

	}





	/**
	 * 
	 * Datatiedoston kirjoitus.
	 * 
	 * 
	 */
	public boolean writeDataFile(S3Client s3Client, FileSpec outputFile, String data) {
		boolean result = false;

		String path = outputFile.path;
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += outputFile.fileName;
		String fullPath = outputFile.bucket + "/" + path;

		logger.log("Write data, file name = '" + fullPath + "'");

		try {

			byte[] stringByteArray = data.getBytes(this.charset);

			Map<String, String> metadata = new HashMap<>();
			metadata.put("content-type", "text/plain");
			metadata.put("content-length", "" + stringByteArray.length);


			PutObjectRequest putRequest = null;

			putRequest = PutObjectRequest.builder()
				.bucket(outputFile.bucket)
				.key(path)
				.metadata(metadata)
				.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
				.build();
			s3Client.putObject(putRequest, RequestBody.fromBytes(stringByteArray));

			result = true;

		} catch (UnsupportedEncodingException e) {
			String errorMessage = "Error: encode '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		} catch (Exception e) {
			String errorMessage = "Error: S3 write '" + e.toString() + "', '" + e.getMessage() + "', file name = '" + fullPath + "'";
			this.logger.log(errorMessage);
			System.err.println(errorMessage);
			e.printStackTrace();
		}
		logger.log("Write data, file name = '" + fullPath + "' => result = " + result);
		return result;

	}

	public String getLastPart(String path) {
		String[] parts = path.split("/");
		String lastPart = parts[parts.length - 1];
		return lastPart.isEmpty() && parts.length > 1 ? parts[parts.length - 2] : lastPart;
	}

}