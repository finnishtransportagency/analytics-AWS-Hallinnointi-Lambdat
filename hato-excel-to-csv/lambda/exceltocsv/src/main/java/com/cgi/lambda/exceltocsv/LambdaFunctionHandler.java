package com.cgi.lambda.exceltocsv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

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


//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3Client;
//import com.amazonaws.services.s3.model.CanonicalGrantee;
//import com.amazonaws.services.s3.model.Grant;
//import com.amazonaws.services.s3.model.Permission;
//import com.amazonaws.services.s3.model.PutObjectRequest;
//import com.amazonaws.services.s3.model.GetObjectRequest;
//import com.amazonaws.services.s3.model.S3Object;
//import com.amazonaws.services.s3.model.AccessControlList;
//import com.amazonaws.services.s3.model.ObjectMetadata;

import org.joda.time.DateTime;



/*
01 hato_tilanne:
	"Yhteenvetokortti_CSV"
	sheet:	""
	mask:	"yhteenvetokortti csv"
	out:	"table.hato_tilanne.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"

02 hato_liik_otot:
	"Yhteenvetokortti_CSV_liikenteelleotot"
	sheet:	""
	mask: 	"yhteenvetokortti csv liikenteelleotot"
	out:	"table.hato_liik_otot.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"

03 hato_tie:
	"Koonti_PVP_tie_hankkeet_CSV"
	sheet:	""
	mask: 	"koonti pvp tie hankkeet csv"
	out:	"table.hato_tie.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"

04 hato_teema:
	"Koonti_PVP_rata_teemat_CSV"
	sheet:	"*"
	mask: 	"koonti pvp rata teemat csv"
	out:	"table.hato_teema.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"

05 hato_rata:
	"Koonti_PVP_rata_hankkeet_CSV"
	sheet:	""
	mask: 	"koonti pvp rata hankkeet csv"
	out:	"table.hato_rata.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"

06 hato_kehhanke:
	"Koonti_keh_ja_Kimolan_kanava_CSV"
	sheet:	""
	mask: 	"koonti keh ja kimolan kanava csv"
	out:	"table.hato_kehhanke.<timestamp>.fullscanned.true.delim.semicolon.skiph.1.csv"
*/




public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	// Region pitää antaa, kai sen saisi kaivettua muutenkin
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
	private boolean includeYearMonth = true;

	private String outputBucket = null;
	private String outputPath = null;
	private String outputArn = null;

	private String archiveBucket = null;
	private String archivePath = null;




		




	@Override
	public String handleRequest(S3Event event, Context context) {

		WorkbookFactory.addProvider(new HSSFWorkbookFactory());
		WorkbookFactory.addProvider(new XSSFWorkbookFactory());

		this.context = context;
		this.logger = new SimpleLambdaLogger(this.context);

		// Vuosikuukausi
		this.runYearMonth = DateTime.now().toString("YYYY-MM");
		String t = System.getenv("add_path_ym");
		if ("0".equals(t) || "false".equalsIgnoreCase(t)) {
			this.includeYearMonth = false;
		}

		// Regioona
		this.region = System.getenv("region");

		// Kohde
		this.outputArn = System.getenv("output_arn");
		this.outputBucket = System.getenv("output_bucket");
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

		S3Entity s3Entity = event.getRecords().get(0).getS3();
		String sourceBucket = s3Entity.getBucket().getName();
		String sourceKey = s3Entity.getObject().getKey();

		String[] sourceKeyPieces = sourceKey.split("/");
		String sourceFileName = sourceKeyPieces[sourceKeyPieces.length - 1].toLowerCase();
		this.logger.log("sourceFileName is: " + sourceFileName);
		String sourceFileNameWithoutTimestamp = sourceFileName;

		//private static Pattern TIMESTAMP_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{2}-\\d{3}$");

		// Odotetaan tiedostoa muodossa hato_kehhanke_2023-01-04-14-07-05-197.xlsx

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

		try {
			int sepPos;
			sepPos = sourceFileName.lastIndexOf("_");
			sourceFileNameWithoutTimestamp = sourceFileName.substring(0, sepPos);
		} catch (StringIndexOutOfBoundsException e) {
			this.logger.log("Missing ' _ ' in: " + sourceFileName);
			this.logger.log("Timestamp missing.");
		}
		sourceFileNameWithoutTimestamp = sourceFileNameWithoutTimestamp + "." + this.fileType;
		this.logger.log("sourceFileNameWithoutTimestamp is: " + sourceFileNameWithoutTimestamp);
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



		String baseName = FilenameUtils.getBaseName(sourceFileNameWithoutTimestamp);
		String logThisThing = ("Kasitellaan tiedosto: " + baseName + "." + this.fileType);

		if(baseName.equals("hato_kehhanke")){
			this.logger.log(logThisThing);
		}
		else if (baseName.equals("hato_liik_otot")) {
			this.logger.log(logThisThing);
		}
		else if (baseName.equals("hato_rata")) {
			this.logger.log(logThisThing);
		}
		else if (baseName.equals("hato_teema")) {
			this.logger.log(logThisThing);
		}
		else if (baseName.equals("hato_tie")) {
			this.logger.log(logThisThing);
		}
		else if (baseName.equals("hato_tilanne")) {
			this.logger.log(logThisThing);
		}
		else{
			this.logger.log("The file " + FilenameUtils.getBaseName(sourceFileNameWithoutTimestamp) + "." + this.fileType + " is not correctly named => Exit");
			return "";
		}

		if (sourceBucket.equals(this.archiveBucket) && sourceKey.startsWith(this.archivePath)) {
			this.logger.log("Do not process archive path => Exit");
			return "";
		}


		String targetName = "";
		String sheet = "";

		int index = 1;
		while(index < 100) {
			t = System.getenv("sourcefile." + index + ".mask");
			if (t != null) {
				if (t.length() > 0) {
					if (sourceFileName.contains(t)) {
						targetName = System.getenv("sourcefile." + index + ".target");
						sheet = System.getenv("sourcefile." + index + ".sheet");
					}
				}
			} else {
				index = 100;
			}
			index++;
		}


		// Lähteen luku
		/*
		v1
		AmazonS3 s3Client = AmazonS3Client.builder().withRegion(this.region).build();
		S3Object object = s3Client.getObject(new GetObjectRequest(sourceBucket, sourceKey));
		InputStream in = object.getObjectContent();
		*/

		S3Client s3Client = S3Client.builder().build();
		GetObjectRequest getRequest = GetObjectRequest.builder().bucket(sourceBucket).key(sourceKey).build();
		InputStream in = s3Client.getObject(getRequest);
		//File excelFile = new File(sourceKey);

		String sheetNames[] = null;
		//this.logger.log("Sheet: " + sheet);
		if (sheet.length() > 0) {
			sheetNames = new String[1];
			sheetNames[0] = sheet;
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
			/*
			v1
			s3Client.copyObject(sourceBucket, sourceKey, targetBucket, targetKey);
			s3Client.deleteObject(sourceBucket, sourceKey);
			*/

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
	public FileSpec makeDataFileName(String name) {
		FileSpec retval = new FileSpec();
		retval.bucket = this.outputBucket;
		retval.path = this.outputPath;
		retval.path += name + "/";
		if (this.includeYearMonth) {
			if (!retval.path.endsWith("/")) retval.path += "/";
			retval.path += this.runYearMonth + "/";
		}
		retval.timestamp = "" + DateTime.now().getMillis();
		retval.sourceName =  name;
		retval.fileName = "table." + name + "." + retval.timestamp + ".fullscanned.true.delim.semicolon.skiph.1.csv";
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

			// v1
			//InputStream byteString = new ByteArrayInputStream(stringByteArray);
			//ObjectMetadata objMetadata = new ObjectMetadata();
			//objMetadata.setContentType("plain/text");
			//objMetadata.setContentLength(stringByteArray.length);

			Map<String, String> metadata = new HashMap<>();
			metadata.put("content-type", "text/plain");
			metadata.put("content-length", "" + stringByteArray.length);


			if (this.outputArn != null) {
				this.outputArn = "";
			}

			//if (this.outputArn.length() > 0) {
			//	// Kirjoitus ja oikeudet
			//	PutObjectRequest request = new PutObjectRequest(outputFile.bucket, path, byteString, objMetadata);
			//	Collection<Grant> grantCollection = new ArrayList<Grant>();
			//
			//	grantCollection.add( new Grant(new CanonicalGrantee(s3Client.getS3AccountOwner().getId()), Permission.FullControl));
			//	grantCollection.add( new Grant(new CanonicalGrantee(this.outputArn), Permission.FullControl));
			//	
			//	AccessControlList objectAcl = new AccessControlList();
			//	objectAcl.getGrantsAsList().clear();
			//	objectAcl.getGrantsAsList().addAll(grantCollection);
			//	request.setAccessControlList(objectAcl);
			//	s3Client.putObject(request);
			//} else {
			//	// Vain kirjoitus
			//	s3Client.putObject(outputFile.bucket, path, byteString, objMetadata);
			//}

			PutObjectRequest putRequest = null;

			putRequest = PutObjectRequest.builder()
				.bucket(outputFile.bucket)
				.key(path)
				.metadata(metadata)
				.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL)
				.build();
			s3Client.putObject(putRequest, RequestBody.fromBytes(stringByteArray));

			if (this.outputArn.length() > 0) {
				PutObjectAclRequest aclRequest = PutObjectAclRequest.builder()
				.bucket(outputFile.bucket)
				.key(path)
				.grantFullControl(this.outputArn)
				.build();
				s3Client.putObjectAcl(aclRequest);
				//.putObject(putRequest, RequestBody.fromBytes(stringByteArray));

			}


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
