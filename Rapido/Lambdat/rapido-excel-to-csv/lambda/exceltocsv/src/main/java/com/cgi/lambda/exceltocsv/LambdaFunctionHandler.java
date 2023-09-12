package com.cgi.lambda.exceltocsv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;

import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.poi.extractor.ExtractorFactory;
import org.apache.poi.extractor.MainExtractorFactory;
import org.apache.poi.hssf.usermodel.HSSFWorkbookFactory;
import org.apache.poi.ooxml.extractor.POIXMLExtractorFactory;
import org.apache.poi.sl.usermodel.SlideShowFactory;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.xslf.usermodel.XSLFSlideShowFactory;
import org.apache.poi.xssf.usermodel.XSSFWorkbookFactory;
import org.apache.commons.io.FilenameUtils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3Entity;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.MetadataEntry;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectAclRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;


import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	// Region taitaa olla turhaa infoa
	private String region = null;

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
	private boolean includeYearMonth = false;

	private String outputBucket = null;
	private String outputPath = null;
	private String outputPrefix = null;

	private String archiveBucket = null;
	private String archivePath = null;

	private String listOfFiles = null;

	private String listOfSheets = null;
	private String masterListOfSheets = null;

	private String masterExcel = null;

	private int masterSkipheaders = 0;

	private boolean master = false;

	private String modifiedAt = null;

	private boolean processOnlyListedFiles = false;


	
	
	private DateTimeZone zone = DateTimeZone.forID("Europe/Helsinki");
	private DateTime today = DateTime.now(zone);
	private DateTime yesterday = today.minusDays(1);
	private int yesterdayYear = yesterday.getYear();
	private int yesterdayMonth = yesterday.getMonthOfYear();

	private String firstDate = null;
	private String lastDate = null;
	private boolean isBetweenDates = true;

	private String baseName = "";

	private S3Entity s3Entity;
	private String sourceBucket;
	private String sourceKey;

	private String[] sourceKeyPieces;
	private String sourceFileName;
	private String sourceFileNameWithoutTimestamp;




	@Override
	public String handleRequest(S3Event event, Context context) {

		WorkbookFactory.addProvider(new HSSFWorkbookFactory());
		WorkbookFactory.addProvider(new XSSFWorkbookFactory());

		DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("dd.MM");

		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);

		this.logger.log("Timezone: " + today.getZone());
		this.logger.log("Timestamp: " + today.toString());
		this.logger.log("Date: " + today.getDayOfMonth() + "." + today.getMonthOfYear() + "." + today.getYear() + ", time: " + today.getHourOfDay() + ":" + today.getMinuteOfHour() + ":" + today.getSecondOfMinute());


		// Vuoden ensimmainen ja viimeinen kasittelypaivamaara
		this.firstDate = System.getenv("first_date");
		this.lastDate = System.getenv("last_date");

		int todayDay = this.today.getDayOfMonth();
        int todayMonth = this.today.getMonthOfYear();

		DateTime comparisonFirstDate = null;
		if (this.firstDate == null || this.firstDate.isEmpty()){
			this.logger.log("No first date given.");
		}
		else{
			// Parse the string firstDate into a comparable date format
			try {
                comparisonFirstDate = dateFormatter.parseDateTime(this.firstDate);
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
			}
			// Compare the current date with the parsed date
			if (todayMonth > comparisonFirstDate.getMonthOfYear() || (todayMonth == comparisonFirstDate.getMonthOfYear()  && todayDay >= comparisonFirstDate.getDayOfMonth() )) {
				this.logger.log("Success: Current date " + todayDay + "." + todayMonth + " is equal to or later than " + comparisonFirstDate.getDayOfMonth() + "." + comparisonFirstDate.getMonthOfYear());
			} else {
				this.logger.log("Failure: Current date " + todayDay + "." + todayMonth + " is earlier than " + comparisonFirstDate.getDayOfMonth() + "." + comparisonFirstDate.getMonthOfYear());
				this.isBetweenDates = false;
			}
		}
		DateTime comparisonLastDate = null;
		if (this.lastDate == null || this.lastDate.isEmpty()){
			this.logger.log("No last date given.");
		}
		else{
			// Parse the string lastDate into a comparable date format
			try {
				comparisonLastDate = dateFormatter.parseDateTime(this.lastDate);
			} catch (IllegalArgumentException e) {
                e.printStackTrace();
			}
			// Compare the current date with the parsed date
			if (todayMonth < comparisonLastDate.getMonthOfYear() || (todayMonth == comparisonLastDate.getMonthOfYear()  && todayDay <= comparisonLastDate.getDayOfMonth() )) {
				this.logger.log("Success: Current date " + todayDay + "." + todayMonth + " is equal to or before than " + comparisonLastDate.getDayOfMonth() + "." + comparisonLastDate.getMonthOfYear());
			} else {
				this.logger.log("Failure: Current date " + todayDay + "." + todayMonth + " is later than " + comparisonLastDate.getDayOfMonth() + "." + comparisonLastDate.getMonthOfYear());
				this.isBetweenDates = false;
			}
		}

		// Kasiteltavat tiedostot
		this.listOfFiles = System.getenv("list_of_files");
		if (this.listOfFiles == null || this.listOfFiles == ""){
			this.logger.log("The lambda has not been set to process any files. Add filenames to list_of_files environment variable.");
			this.logger.log("Use semicolon followed by space to separate filenames and don't include file extension.");
			this.logger.log("Nothing to do here. Terminating lambda.");
			return "";
		}

		// Kasiteltavat valilehdet
		this.listOfSheets = System.getenv("list_of_sheets");
		if (this.listOfSheets == null || this.listOfSheets == ""){
			this.logger.log("The lambda has not been set to process any specific sheets. All sheets will be included in csv.");
			this.logger.log("If you want to set sheetnames to process, add filenames to list_of_sheets environment variable.");
			this.logger.log("Use semicolon followed by space to separate sheetnames.");
		}
		// Master Excelin kasiteltavat valilehdet
		this.masterListOfSheets = System.getenv("master_list_of_sheets");
		if (this.masterListOfSheets == null || this.masterListOfSheets == ""){
			this.logger.log("Master list of sheets not specified, using the regular list of sheets for master file");
			this.masterListOfSheets = this.listOfSheets;
		}

		// Vuosikuukausi (ja paiva)
		this.runYearMonth = this.today.toString("yyyy/MM/dd");
		String t = System.getenv("add_path_ym");
		if ("1".equals(t) || "true".equalsIgnoreCase(t) ) {
			this.includeYearMonth = true;
		}

		// Master Excelin nimi
		this.masterExcel = System.getenv("master_excel");

		// Montako riviä Master Excelissa skipataan
		t = System.getenv("master_skipheaders");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.masterSkipheaders = Integer.valueOf(t).intValue();
		}

		// Regioona
		this.region = System.getenv("AWS_REGION");

		// Kohde
		this.outputBucket = System.getenv("output_bucket");
		this.outputPrefix = System.getenv("output_prefix");
		this.outputPath = System.getenv("output_path");
		if (!this.outputPath.equals("")) {
			if (!this.outputPath.endsWith("/")){
				this.outputPath += "/";
			}
		}
		// Arkisto
		this.archiveBucket = System.getenv("archive_bucket");
		if (this.archiveBucket == null) this.archiveBucket = "";
		this.archivePath = System.getenv("archive_path");
		if (this.archivePath == null) {
			if (!this.archivePath.endsWith("/")) {
				this.archivePath += "/";
			}
		}
		

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

		t = System.getenv("process_only_listed_files");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.processOnlyListedFiles = true;
			} else {
				this.processOnlyListedFiles = false;
			}
		}

		this.s3Entity = event.getRecords().get(0).getS3();
		this.sourceBucket = this.s3Entity.getBucket().getName();
		this.sourceKey = this.s3Entity.getObject().getKey();

		this.sourceKeyPieces = this.sourceKey.split("/");
		this.sourceFileName = this.sourceKeyPieces[sourceKeyPieces.length - 1].toLowerCase();
		this.logger.log("sourceFileName is: " + this.sourceFileName);
		this.sourceFileNameWithoutTimestamp = this.sourceFileName;

		if (!sourceFileName.endsWith(".xls") && !sourceFileName.endsWith(".xlsx")) {
			this.logger.log("Not an excel file (" + this.sourceBucket + this.sourceKey +  "). Nothing to do => Exit");
			return "";
		}
		else if (this.sourceFileName.endsWith(".xls")) {
			this.fileType = "xls";
		}
		else{
			this.fileType = "xlsx";
		}

		try {
			int sepPos;
			sepPos = this.sourceFileName.lastIndexOf("_");
			this.sourceFileNameWithoutTimestamp = this.sourceFileName.substring(0, sepPos);
			this.modifiedAt = this.sourceFileName.substring(sepPos+1, (this.sourceFileName.length()-(fileType.length()+1)));
			this.logger.log("ModifiedAtTimestamp is: " + modifiedAt);

		} catch (StringIndexOutOfBoundsException e) {
			this.logger.log("Missing ' _ ' in: " + this.sourceFileName);
			this.logger.log("Timestamp missing.");
		}
		this.sourceFileNameWithoutTimestamp = this.sourceFileNameWithoutTimestamp + "." + this.fileType;
		this.logger.log("sourceFileNameWithoutTimestamp is: " + this.sourceFileNameWithoutTimestamp);
		/* 
		this.logger.log("sourceFileNameWithoutTimestamp (and extension) is: " + sourceFileNameWithoutTimestamp);
		this.logger.log("sourceFileName is: " + sourceFileName);
		try {
			this.fileType = sourceFileName.split(".")[1];
			sourceFileNameWithoutTimestamp = sourceFileNameWithoutTimestamp + "." + this.fileType;
		} catch (ArrayIndexOutOfBoundsException e) {
			this.logger.log("Missing ' . ' in: " + sourceFileName);
			this.logger.log("File extension is missing.");
		}
		this.logger.log("sourceFileNameWithoutTimestamp is: " + sourceFileNameWithoutTimestamp);
		*/

		if (!this.isBetweenDates){
			this.logger.log("Paivamaara on kasittelyajan ulkopuolella...");
			// Lähteen arkistointi, arkistopolkuun lisätään päivämäärä (yyyyMMDD) ja tiedoston nimen eteen siirtoaikaleima (yyyyMMddhhmmss)
			if (this.archiveBucket.length() > 0) {
				this.logger.log("Arkistoidaan tiedosto...");
				S3Client s3Client = S3Client.builder().build();
				DateTimeFormatter formatt = DateTimeFormat.forPattern("yyyyMMdd");
				this.logger.log("");
				this.moveSourceFile(s3Client, this.sourceBucket, this.sourceKey, this.archiveBucket, this.archivePath + formatt.print(today) + "/" + this.sourceFileName);
			}
			this.logger.log("Tiedostoa, ei kasitella. Lopetetaan lambda...");
			return "";
		}

		this.baseName = FilenameUtils.getBaseName(this.sourceFileNameWithoutTimestamp);
		String logThisThing = ("Kasitellaan tiedosto: " + this.baseName + "." + this.fileType);
		// this.logger.log("Checking if filename: " + this.baseName + " equals " + " master_excel: " + this.masterExcel.toLowerCase());
		
		if (this.masterExcel == null || this.masterExcel == ""){
			this.logger.log("Master excel not defined");
			this.master = false;
		}
		else{
			String[] masterFilesToProcess = this.masterExcel.split("; ");	

			for (String masterName : masterFilesToProcess) {
				this.logger.log("Checking if " + this.baseName + " equals " + masterName.toLowerCase());
				if (this.baseName.equals(masterName.toLowerCase())){
					this.master = true;
					this.skipheaders = this.masterSkipheaders;
					this.listOfSheets = this.masterListOfSheets;
					break;
				}
				else{
					this.master = false;
				}
			}
			this.logger.log("Master tiedosto: " + this.master);

		}

		
		String[] sheetsToProcess = this.listOfSheets.split("; ");

		if (this.processOnlyListedFiles){
			String[] filesToProcess = this.listOfFiles.split("; ");
			boolean nameIsOnTheList = false;
			for (String s : filesToProcess){
				if (this.baseName.equals(s.toLowerCase())){
					this.logger.log("Nimi on kasiteltavien tiedostojen listalla.");
					nameIsOnTheList = true;
				}
			}

			if (nameIsOnTheList){
				this.logger.log(logThisThing);
			}
			else {
				this.logger.log("The file " + this.baseName.toUpperCase() + " is not on the list of files to process => Exit");
				return "";
			}
		}
		else{
			this.logger.log("Kasiteltavia tiedostoja ei ole erikseen maaritelty.");
			this.logger.log(logThisThing);
		}		
		if (this.sourceBucket.equals(this.archiveBucket) && this.sourceKey.startsWith(this.archivePath)) {
			this.logger.log("Do not process archive path => Exit");
			return "";
		}
		

		

		String sheet = "";

		// Lähteen luku
		S3Client s3Client = S3Client.builder().build();
		GetObjectRequest getRequest = GetObjectRequest.builder().bucket(this.sourceBucket).key(this.sourceKey).build();
		InputStream in = s3Client.getObject(getRequest);

		String sheetNames[] = null;
		//this.logger.log("Sheet: " + sheet);
		if (sheet.length() > 0) {
			sheetNames = new String[1];
			sheetNames[0] = sheet;
		}

		if (sheetsToProcess.length>0){
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
			converter.setSourceKey(this.sourceKey);
			converter.setYear(this.yesterdayYear);
			converter.setMonth(this.yesterdayMonth);
			converter.setMaster(this.master);
			converter.setModifiedAt(this.modifiedAt);
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
			FileSpec outputFile = makeDataFileName(this.baseName, master);
			this.writeDataFile(s3Client, outputFile, data);

			// Lähteen arkistointi, arkistopolkuun lisätään päivämäärä (yyyyMMDD) ja tiedoston nimen eteen siirtoaikaleima (yyyyMMddhhmmss)
			if (this.archiveBucket.length() > 0) {
				///DateTime dateTimeOfOutputFile = new DateTime(Long.parseLong(outputFile.timestamp));
				//String today = dateTimeOfOutputFile.toString("yyyyMMdd");
				this.moveSourceFile(s3Client, this.sourceBucket, this.sourceKey, this.archiveBucket, this.archivePath + this.today.toString("yyyyMMdd") + "/" + outputFile.timestamp + " " + this.sourceFileName);
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
	public FileSpec makeDataFileName(String name, boolean isMaster) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;
		if (this.outputPrefix != null){
			retval.prefix = this.outputPrefix;
		}
		if (isMaster){
			retval.path += retval.prefix + "pvp_budjetointi_master/";
			retval.fullscanned = true;
		}
		
		else{
			retval.path += retval.prefix + "pvp_ohjelmointilomakkeet/";
			retval.fullscanned = false;
		}
	
		if (this.includeYearMonth) {
			if (!retval.path.endsWith("/")) retval.path += "/";
			retval.path += this.runYearMonth + "/";
		}
		retval.timestamp = "" + DateTime.now().getMillis();
		retval.sourceName =  name;
		retval.fileName = "table." + retval.prefix + name + "." + retval.timestamp + ".fullscanned." + retval.fullscanned +".delim.semicolon.skiph.1.csv";
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

}