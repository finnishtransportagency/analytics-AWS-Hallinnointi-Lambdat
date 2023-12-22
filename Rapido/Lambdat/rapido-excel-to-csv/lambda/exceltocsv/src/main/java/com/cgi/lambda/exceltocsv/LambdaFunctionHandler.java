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

	private String alarmTopic = "";
	
	
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

	private String kausi = "";
	private boolean korjaustiedosto = false;


	@Override
	public String handleRequest(S3Event event, Context context) {


		// Initialize Excel workbook factories for HSSF (xls) and XSSF (xlsx) formats
		WorkbookFactory.addProvider(new HSSFWorkbookFactory());
		WorkbookFactory.addProvider(new XSSFWorkbookFactory());

		// Define date formatter for processing dates (we need it for the comparison)
		DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("dd.MM");

		// Set up logger and context
		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);

		// Log current date and time information
		this.logger.log("Timezone: " + today.getZone());
		this.logger.log("Timestamp: " + today.toString());
		this.logger.log("Date: " + today.getDayOfMonth() + "." + today.getMonthOfYear() + "." + today.getYear() + ", time: " + today.getHourOfDay() + ":" + today.getMinuteOfHour() + ":" + today.getSecondOfMinute());

		  ///////////////////////////
		 // Environment variables //
		///////////////////////////

		// First and last processing dates of the year from environment variables, in the format D.M, for example, 8.1 or 20.12
		this.firstDate = System.getenv("first_date");
		this.lastDate = System.getenv("last_date");

		// Files to be processed
		this.listOfFiles = System.getenv("list_of_files");
		if (this.listOfFiles == null || this.listOfFiles == ""){
			this.logger.log("The lambda has not been set to process any files. Add filenames to list_of_files environment variable.");
			this.logger.log("Use semicolon followed by space to separate filenames and don't include file extension.");
			this.logger.log("Nothing to do here. Terminating lambda.");
			return "";
		}

		// Sheets to be processed
		this.listOfSheets = System.getenv("list_of_sheets");
		if (this.listOfSheets == null || this.listOfSheets == ""){
			this.logger.log("The lambda has not been set to process any specific sheets. All sheets will be included in csv.");
			this.logger.log("If you want to set sheetnames to process, add filenames to list_of_sheets environment variable.");
			this.logger.log("Use semicolon followed by space to separate sheetnames.");
		}
		// Sheets to be processed from the Master Excel
		this.masterListOfSheets = System.getenv("master_list_of_sheets");
		if (this.masterListOfSheets == null || this.masterListOfSheets == ""){
			this.logger.log("Master list of sheets not specified, using the regular list of sheets for master file");
			this.masterListOfSheets = this.listOfSheets;
		}

		// Year-month (and day)
		this.runYearMonth = this.today.toString("yyyy/MM/dd");
		String t = System.getenv("add_path_ym");
		if ("1".equals(t) || "true".equalsIgnoreCase(t) ) {
			this.includeYearMonth = true;
		}

		// Name of the Master Excel
		this.masterExcel = System.getenv("master_excel");

		// Number of header rows to skip in the input data
		t = System.getenv("skipheaders");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.skipheaders = Integer.valueOf(t).intValue();
		}

		// Number of rows to skip in the Master Excel
		t = System.getenv("master_skipheaders");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.masterSkipheaders = Integer.valueOf(t).intValue();
		}

		// Region
		this.region = System.getenv("AWS_REGION");

		// Destination
		this.outputBucket = System.getenv("output_bucket");
		this.outputPrefix = System.getenv("output_prefix");
		this.outputPath = System.getenv("output_path");
		if (!this.outputPath.equals("")) {
			if (!this.outputPath.endsWith("/")){
				this.outputPath += "/";
			}
		}
		// Archive
		this.archiveBucket = System.getenv("archive_bucket");
		if (this.archiveBucket == null) this.archiveBucket = "";
		this.archivePath = System.getenv("archive_path");
		if (this.archivePath == null) {
			if (!this.archivePath.endsWith("/")) {
				this.archivePath += "/";
			}
		}
		
		// Alarm topic for error messages
		t = System.getenv("alarm_topic");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.alarmTopic = t;
		}

		// Flag indicating whether to process only the listed files or all files
		t = System.getenv("process_only_listed_files");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.processOnlyListedFiles = true;
			} else {
				this.processOnlyListedFiles = false;
			}
		}

		   /////////////////////////////////////////////////////////////////////////////////////////////////////////
		  // Converter parameters, generally no need to change from defaults. We don't even currently have these //
		 //  as AWS Lambda environment variables on this project.												//
		/////////////////////////////////////////////////////////////////////////////////////////////////////////

		// Character set for encoding/decoding text data
		t = System.getenv("charset");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.charset = t;
		}
		// Delimiter used to separate fields in the input data
		t = System.getenv("delimiter");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.delimiter = t;
		}
		// End-of-line (EOL) character or sequence in the input data
		t = System.getenv("eol");
		if (t == null) t = "";
		if (t.length() > 0) {
			// Translate common escape sequences to actual characters
			if (t.equalsIgnoreCase("crlf") || t.equalsIgnoreCase("\\r\\n")) {
				this.eol = "\r\n";
			} else if (t.equalsIgnoreCase("cr") || t.equalsIgnoreCase("\\r")) {
				this.eol = "\r";
			} else if (t.equalsIgnoreCase("lf") || t.equalsIgnoreCase("\\n")) {
				this.eol = "\n";
			} else if (t.equalsIgnoreCase("lfcr") || t.equalsIgnoreCase("\\n\\r")) {
				this.eol = "\n\r";
			} else {
				// Use the provided EOL character/sequence as is
				this.eol = t;
			}
		}
		// Flag indicating whether the input data has a header row
		t = System.getenv("hasheader");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.hasHeader = true;
			} else {
				this.hasHeader = false;
			}
		}
		// Quote character used to enclose fields in the input data
		t = System.getenv("quote");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.quote = t;
		}
		// Quote escape character used to escape quotes in the input data
		t = System.getenv("quoteescape");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.quoteEscape = t;
		}
		// String to replace carriage return (CR) characters in the input data
		t = System.getenv("replacecr");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.replaceCR = t;
		}
		// String to replace newline (NL) characters in the input data
		t = System.getenv("replacenl");
		if (t == null) t = "";
		if (t.length() > 0) {
			this.replaceNL = t;
		}
		// Flag indicating whether to trim whitespace from data fields
		t = System.getenv("trimdata");
		if (t == null) t = "";
		if (t.length() > 0) {
			if ("1".equals(t) || "true".equalsIgnoreCase(t)) {
				this.trimData = true;
			} else {
				this.trimData = false;
			}
		}

		//////////////////////////////////////////////////////////////////////////////////////////////////////

		// Current day and month
		int todayDay = this.today.getDayOfMonth();
        int todayMonth = this.today.getMonthOfYear();

		// Comparison date for the first processing date
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
		// Comparison date for the last processing date
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

		// Extracting source key and bucket information from S3 event
		this.s3Entity = event.getRecords().get(0).getS3();
		this.sourceBucket = this.s3Entity.getBucket().getName();
		this.sourceKey = this.s3Entity.getObject().getKey();

		// Parsing source key to extract file information
		this.sourceKeyPieces = this.sourceKey.split("/");
		this.sourceFileName = this.sourceKeyPieces[sourceKeyPieces.length - 1].toLowerCase();

		// Checking if the source file name starts with "kausi_" (season_)
		if (this.sourceFileName.startsWith("kausi_")){
			try {
				// Extracting season information from the source file name
				this.kausi = this.sourceFileName.split("_")[1];
				if (this.kausi.length() == 7) {
					this.logger.log("Korjaustiedosto. Paivitetaan tiedot kaudelle: " + this.kausi);
					this.korjaustiedosto = true;
				}
			}
			catch (Exception e) {
					this.logger.log("Error parsing 'kausi_YYYY-MM_': '" + e.getMessage() + "'");
				}	
		}

		// Logging source file name information
		this.logger.log("sourceFileName is: " + this.sourceFileName);
		this.sourceFileNameWithoutTimestamp = this.sourceFileName;

		// Checking file type based on the file extension
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
			// Find the last occurrence of "_" in the source file name to separate the timestamp
			int sepPos;
			sepPos = this.sourceFileName.lastIndexOf("_");

			// Extract the base source file name based on the "_" separator
			this.sourceFileNameWithoutTimestamp = this.sourceFileName.substring(0, sepPos);
			this.modifiedAt = this.sourceFileName.substring(sepPos+1, (this.sourceFileName.length()-(fileType.length()+1)));
			this.logger.log("ModifiedAtTimestamp is: " + modifiedAt);

		} catch (StringIndexOutOfBoundsException e) {
			this.logger.log("Missing ' _ ' in: " + this.sourceFileName);
			this.logger.log("Timestamp missing.");
		}
		// Append the file type to the source file name without timestamp
		this.sourceFileNameWithoutTimestamp = this.sourceFileNameWithoutTimestamp + "." + this.fileType;
		this.logger.log("sourceFileNameWithoutTimestamp is: " + this.sourceFileNameWithoutTimestamp);

		// Check if the current date is within the specified processing dates
		if (!this.isBetweenDates){
			this.logger.log("Paivamaara on kasittelyajan ulkopuolella...");
			// Archive the source file with a timestamped path
			if (this.archiveBucket.length() > 0) {
				this.logger.log("Arkistoidaan tiedosto...");
				S3Client s3Client = S3Client.builder().build();
				DateTimeFormatter formatt = DateTimeFormat.forPattern("yyyyMMdd");
				this.logger.log("");
				// Move the source file to the archive path with timestamped directories
				this.moveSourceFile(s3Client, this.sourceBucket, this.sourceKey, this.archiveBucket, this.archivePath + "datakatko/" + formatt.print(today) + "/" + this.sourceFileName);
			}
			this.logger.log("Tiedostoa ei kasitella. Lopetetaan lambda...");
			return "";
		}

		// Get the base name of the source file without timestamp
		this.baseName = FilenameUtils.getBaseName(this.sourceFileNameWithoutTimestamp);
		// Log the processing of the file
		String logThisThing = ("Kasitellaan tiedosto: " + this.baseName + "." + this.fileType);

		// Check if the master Excel file is defined
		if (this.masterExcel == null || this.masterExcel == ""){
			this.logger.log("Master excel not defined");
			this.master = false;
		}
		else{
			// Split the master Excel file names (list from environment variable)
			String[] masterFilesToProcess = this.masterExcel.split("; ");	

			// Iterate through the master Excel file names
			for (String masterName : masterFilesToProcess) {
				this.logger.log("Checking if " + this.baseName + " equals " + masterName.toLowerCase());
				
				// Check if the base name matches a master Excel file name
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

		// Split the list of sheets to process (list from environment variable)
		String[] sheetsToProcess = this.listOfSheets.split("; ");

		// Check if processing only listed files
		if (this.processOnlyListedFiles){

			// Split the list of files to process
			String[] filesToProcess = this.listOfFiles.split("; ");
			boolean nameIsOnTheList = false;

			// Iterate through the list of files to check if the base name is on the list
			for (String s : filesToProcess){
				if (this.baseName.equals(s.toLowerCase())){
					this.logger.log("Nimi on kasiteltavien tiedostojen listalla.");
					nameIsOnTheList = true;
				}
			}

			// Log the processing if the name is on the list, otherwise exit
			if (nameIsOnTheList){
				this.logger.log(logThisThing);
			}
			else {
				this.logger.log("The file " + this.baseName.toUpperCase() + " is not on the list of files to process => Exit");
				return "";
			}
		}
		else{
			// Log a message when no specific files are defined for processing
			this.logger.log("Kasiteltavia tiedostoja ei ole erikseen maaritelty.");
			this.logger.log(logThisThing);
		}	
		// We don't want to process the file if for some reason the lambda was triggered from archive. This could happen,
		//  if we accidentaly configured same archive path and s3 trigger path for the lambda
		if (this.sourceBucket.equals(this.archiveBucket) && this.sourceKey.startsWith(this.archivePath)) {
			this.logger.log("Do not process archive path => Exit");
			return "";
		}
		
		
		// Initialize the sheet variable
		String sheet = "";

		// Set up S3 client
		S3Client s3Client = S3Client.builder().build();

		// Create a GetObjectRequest to retrieve the specified object from S3
		GetObjectRequest getRequest = GetObjectRequest.builder().bucket(this.sourceBucket).key(this.sourceKey).build();

		// Get the input stream for the S3 object
		InputStream in = s3Client.getObject(getRequest);

		// Initialize an array to store sheet names
		String sheetNames[] = null;

		// Not sure if this part is ever used or why it's here, maybe > should be == ?
		//  and maybe this should be after the next part of code...
		if (sheet.length() > 0) {
			sheetNames = new String[1];
			sheetNames[0] = sheet;
		}

		// Check if there are sheets to process from the environment variable
		if (sheetsToProcess.length>0){
			// If sheets are specified, use them for processing
			sheetNames = sheetsToProcess;
			this.logger.log("Sheets to process:");
			for (int i = 0; i < sheetNames.length; i++) {
				this.logger.log(sheetNames[i]);
			}
		}
		

		  ////////////////
		 // Conversion //
		////////////////
		try {
			// Create a ByteArrayOutputStream to store the converted CSV data
			ByteArrayOutputStream out = new ByteArrayOutputStream();

			// Create an instance of XlsToCsvConverter
			XlsToCsvConverter converter = new XlsToCsvConverter();

			// Set various parameters for the converter based on environment variables and file information
			converter.setSourceKey(this.sourceKey);
			if (this.kausi.length() == 7){
				converter.setYear(Integer.parseInt(this.kausi.substring(0, 4)));
				converter.setMonth(Integer.parseInt(this.kausi.substring(5)));
			}
			else {
				converter.setYear(this.yesterdayYear);
				converter.setMonth(this.yesterdayMonth);
				}
			converter.setKorjaustiedosto(this.korjaustiedosto);
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
			this.logger.log("Setting baseName of converter as: " + this.baseName);
			converter.setBaseName(this.baseName);
			converter.setAlarmTopic(this.alarmTopic);
			converter.convert(in, out, this.fileType/*, excelFile*/);
			in.close();

			// Get the CSV data as a string from the ByteArrayOutputStream
			String data = out.toString(this.charset);

			// Close the ByteArrayOutputStream
			out.close();

			// Create a FileSpec for the output CSV file
			FileSpec outputFile = makeDataFileName(this.baseName, master);

			// Write the CSV data to the destination (S3 bucket)
			this.writeDataFile(s3Client, outputFile, data);

			// Archive the source file with a timestamped path if an archive bucket is specified
			if (this.archiveBucket.length() > 0) {
				this.moveSourceFile(s3Client, this.sourceBucket, this.sourceKey, this.archiveBucket, this.archivePath + this.today.toString("yyyyMMdd") + "/" + outputFile.timestamp + " " + this.sourceFileName);
			}

		} catch (Exception e) {
			this.logger.log("Conversion exception: '" + e.toString() + "', '" + e.getMessage() + "'");
		}
		
		return "";
	}

	
	
	/**
	 * Moves a file from a source location in an S3 bucket to a target location in another S3 bucket.
	 *
	 * @param s3Client      The S3 client used for performing S3 operations.
	 * @param sourceBucket  The source S3 bucket.
	 * @param sourceKey     The key (path) of the file in the source bucket.
	 * @param targetBucket  The target S3 bucket.
	 * @param targetKey     The key (path) to which the file will be moved in the target bucket.
	 * @return              true if the move operation is successful, false otherwise.
	 */
	public boolean moveSourceFile(S3Client s3Client, String sourceBucket, String sourceKey, String targetBucket, String targetKey) {
		// Create full paths for source and target locations
		String sourceFullPath = sourceBucket + "/" + sourceKey;
		String targetFullPath = targetBucket + "/" + targetKey;

		// Log the move operation
		this.logger.log("Move '" + sourceFullPath + "' -> '" + targetFullPath + "'");
		
		try {
			// Create a CopyObjectRequest to copy the file from source to target
			CopyObjectRequest copyRequest = CopyObjectRequest.builder()
				.sourceBucket(sourceBucket)
				.sourceKey(sourceKey)
				.destinationBucket(targetBucket)
				.destinationKey(targetKey)
				.build();

			// Copy the object from source to target
			s3Client.copyObject(copyRequest);

			// Create a DeleteObjectRequest to delete the file from the source after copying
			DeleteObjectRequest delRequest = DeleteObjectRequest.builder()
				.bucket(sourceBucket)
				.key(sourceKey).build();

			// Delete the object from the source
			s3Client.deleteObject(delRequest);
			
		} catch (Exception e) {
			// Log an error message if the move operation fails
			this.logger.log("Move '" + sourceFullPath + "' -> '" + targetFullPath + "' failed: '" + e.toString() + "', '" + e.getMessage() + "'");
			return(false); // Return false to indicate failure
		}
		return(true); // Return true to indicate success
	}



	

	/**
	 * Creates a FileSpec object for the output file based on the provided name and whether it is a master file.
	 * 
	 * @author Isto Saarinen 2021-12-02 / Arto Lautamo 2023-12-19
	 * 
	 * @param name      The base name for the output file
	 * @param isMaster  Indicates whether the file is a master file.
	 * @return 			A FileSpec object representing the details of the output file. (file and path)
	 */
	public FileSpec makeDataFileName(String name, boolean isMaster) {
		// Create a new FileSpec object
		FileSpec retval = new FileSpec();

		// Set common attributes for both master and non-master files
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;

		// Set the prefix if available
		if (this.outputPrefix != null){
			retval.prefix = this.outputPrefix;
		}

		// Determine the path based on whether it's a master file or not
		if (isMaster){
			retval.path += retval.prefix + "pvp_budjetointi_master/";
			retval.fullscanned = true;
		}
		
		else{
			retval.path += retval.prefix + "pvp_ohjelmointilomakkeet/";
			retval.fullscanned = false;
		}

		// Include year and month in the path if required
		if (this.includeYearMonth) {
			if (!retval.path.endsWith("/")) retval.path += "/";
			retval.path += this.runYearMonth + "/";
		}
		// Set timestamp, source name, and file name
		retval.timestamp = "" + DateTime.now().getMillis();
		retval.sourceName =  name;
		retval.fileName = "table." + retval.prefix + name + "." + retval.timestamp + ".fullscanned." + retval.fullscanned +".delim.semicolon.skiph.1.csv";
		
		// Return the created FileSpec object
		return retval; 

	}





	/**
	 * Writes data to an S3 bucket as a text file with specified metadata.
	 *
	 * @param s3Client      The S3 client for performing the write operation.
	 * @param outputFile    Details of the output file, including bucket, path, and filename.
	 * @param data          The data to be written to the file.
	 * @return              True if the write operation is successful, false otherwise.
	 */
	public boolean writeDataFile(S3Client s3Client, FileSpec outputFile, String data) {
		boolean result = false;

		// Build the full path of the output file
		String path = outputFile.path;
		if (!path.endsWith("/")) {
			path += "/";
		}
		path += outputFile.fileName;
		String fullPath = outputFile.bucket + "/" + path;

		// Log the initiation of the write operation
		logger.log("Write data, file name = '" + fullPath + "'");

		try {

			// Convert data to byte array with specified character set
			byte[] stringByteArray = data.getBytes(this.charset);

			// Set metadata for the S3 object
			Map<String, String> metadata = new HashMap<>();
			metadata.put("content-type", "text/plain");
			metadata.put("content-length", "" + stringByteArray.length);

			// Build the S3 put object request
			PutObjectRequest putRequest = PutObjectRequest.builder()
				.bucket(outputFile.bucket)
				.key(path)
				.metadata(metadata)
				.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
				.build();

			// Perform the S3 put object operation with the data
			s3Client.putObject(putRequest, RequestBody.fromBytes(stringByteArray));

			// Mark the write operation as successful
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
		// Log the result of the write operation
		logger.log("Write data, file name = '" + fullPath + "' => result = " + result);
		return result;

	}

}