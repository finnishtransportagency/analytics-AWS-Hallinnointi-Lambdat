package com.cgi.lambda.exceltocsv;

import java.util.ArrayList;
import java.util.Set;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

import java.io.InputStream;
import java.io.OutputStream;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.poi.ss.formula.functions.Replace;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

import org.joda.time.DateTime;


/**
 *
 * Excel => CSV muunnin
 *
 * Alkuperäinen versio 2013-10-16
 * Korjattu versio 2022-06-01
 * Muutettu kaikille lehdille 2022-11-01
 *
 * Editoitu 2023-12-19
 * 
 * @author isto.saarinen, edit. arto.lautamo
 *
 */
public class XlsToCsvConverter {

	// Constants
	private static final String CONVERT_ALL_SHEETS = "*";
	private static final String NULLSTRING = "null";

	// Configuration variables with default values
	private boolean hasHeader = false;			// Whether the CSV file has a header
	private String replaceCR = "";				// String to replace carriage return
	private String replaceNL = " ";				// String to replace newline
	private String delimiter = ";";				// CSV field delimiter
	private String[] sheetNames = null;			// Names of specific sheets to convert, null means convert all
	private boolean includeSheetName = false;	// Whether to include sheet names in CSV output
	private String charSet = "UTF-8";			// Character encoding for CSV file
	private String eol = System.lineSeparator();// End-of-line sequence
	private String quote = "\"";				// Quote character for CSV fields
	private String quoteEscape = "\"";			// Escape character for quotes within CSV fields
	private String[][] replaceHeaderChars = { {"ä","a"},  {"å","a"}, {"ö","o"}, {" ","_"} };
												// Characters to replace scandinavian characters with in header names
	private boolean trimData = false;			// Whether to trim leading and trailing whitespaces from data
	private int skipheaders = 0;				// Number of header lines to skip
	private String modifiedAt = null;			// Last modified timestamp (from filename)
	private String teema = "";					// Teema
	private String trSelite = "";				// Teeman selite
	private boolean master = false;				// Whether this is a master file
	private int year = 0;						// Year of yesterday (since we are loading after midnight)
	private int month = 0;						// Month of yesterday
	private String sourceKey = "";				// Source key
	private String baseName = "";				// Base name
	private int otsikkoVuosi = 0;				// Year from header row
	private int muokkausVuosi = 0;				// Year from timestamp in filename
	private boolean korjaustiedosto = false;	// Whether this is a correction file
	private int puuttuvatKohdeToimenpiteet = 0; // Number of missing kohde_toimenpide values for existing rivi_id values on a row

	// Logger and error handling objects
	private SimpleLogger logger = null;			// Logger for logging messages
	private ErrorMail mail = new ErrorMail();	// Email utility for sending error notifications

	private String alarmTopic = "";				// Topic for alarm notifications

	// Method to log messages using the logger
	private void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}


	/**
	 * Checks if the given string starts with a UTF-8 minus-sign-like character.
	 * Reference: https://jkorpela.fi/dashes.html
	 *
	 * @param s The input string to check.
	 * @return True if the string starts with a UTF-8 minus-sign-like character, false otherwise.
	 */
	private boolean startsWithSomeDash(String s) {
		boolean startsWithDash = false;

		// List of Unicode code points representing minus-sign-like characters
		int[] dashList = {
				45, 6150, 8208, 8209, 8210, 8211, 8212, 8213, 8315, 8331, 8722,  11834, 11835, 65112, 65123, 65293
		};

		// Get the Unicode code point of the first character in the string
		int c = (int)s.charAt(0);

		// Check if the first character matches any Unicode code point in the dashList
		for (int i : dashList) {
			if (c == i) {
				startsWithDash = true;
				break; // No need to continue checking once a match is found
			}
		}
		return(startsWithDash);
	}

	/**
	 * Replaces UTF-8 minus-sign-like character at the beginning of the string with ASCII minus sign.
	 *
	 * @param s The input string to process.
	 * @return The modified string with the UTF-8 minus-sign-like character replaced by ASCII minus sign.
	 */
	private String replaceDashToAscii(String s) {
		// Check if the string starts with a UTF-8 minus-sign-like character
		boolean startsWithDash = this.startsWithSomeDash(s);

		// Create a copy of the original string
		String result = s;

		// If the string starts with a UTF-8 minus-sign-like character, replace it with ASCII minus sign
		if (startsWithDash) {
			result = ((char)45) + s.substring(1);
		}
		return result;
	}

	/**
	 * Replaces specific non-breaking space characters with regular space characters in the given string.
	 *
	 * @param s The input string to process.
	 * @return The input string with non-breaking space characters replaced.
	 */
	private String replaceNonBreakingSpaces(String s) {
		// Check if the input string is null
		if (s == null) return(null);

		// Replace non-breaking space characters with regular space characters
		s.replaceAll("\u00A0", " ");
		s.replaceAll("\u2007", " ");
		s.replaceAll("\u202F", " ");
		return s;
	}

	/**
	 * Replaces various space characters with regular space characters in the given string.
	 *
	 * @param s The input string to process.
	 * @return The input string with space characters replaced.
	 */
	public static String replaceSpaces(String s) {
		// Check if the input string is null
		if (s == null) return(null);

		// Array of Unicode code points representing different space characters
		int[] spaceList = {
				160, 5760, 6158, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8203, 8239, 8287, 12288, 65279
		};

		// Iterate through the spaceList and replace each space character in the input string
		for(int c: spaceList) {
			s = s.replace((char)c, ' ');
		}
		return(s);
	}

	// Set-functions for variables

	public void setLogger(SimpleLogger logger) {
		this.logger = logger;
	}

	public void setHasHeader(boolean flag) {
		this.hasHeader = flag;
	}

	public void setReplaceCR(String replaceCR) {
		this.replaceCR = replaceCR;
	}

	public void setReplaceNL(String replaceNL) {
		this.replaceNL = replaceNL;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public void setQuote(String quote) {
		this.quote = quote;
	}

	public void setQuoteEscape(String quoteEscape) {
		this.quoteEscape = quoteEscape;
	}

	public void setSheetNames(String[] sheetNames) {
		this.sheetNames = sheetNames;
	}

	public void setCharSet(String charSet) {
		this.charSet = charSet;
	}

	public void setEOL(String eol) {
		this.eol = eol;
	}

	public void setTrimData(boolean trimData) {
		this.trimData = trimData;
	}

	public void setSkipheaders(int skipheaders) {
		this.skipheaders = skipheaders;
	}

	public void setIncludeSheetName(boolean includeSheetName) {
		this.includeSheetName = includeSheetName;
	}

	public void setModifiedAt(String modifiedAt) {
		this.modifiedAt = modifiedAt;
	}

	public void setMaster(boolean master) {
		this.master = master;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public void setSourceKey(String sourceKey) {
		this.sourceKey = sourceKey;
	}

	public void setBaseName(String b) {
		this.baseName = b;
		this.logger.log("baseName set to: " + this.baseName);
	}
	
	public void setKorjaustiedosto(boolean korjaustiedosto) {
		this.korjaustiedosto = korjaustiedosto;
	}

	public void setAlarmTopic(String alarmTopic) {
		this.alarmTopic = alarmTopic;
	}

	// Formats the output value for printing
	private String fixOutputValue(String v) {
		// Ensure the input value is not null
		String s = (v != null) ? v : "";

		// Replace NULLSTRING with an empty string
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}

		// Escape quotes if the value contains the quote character
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, this.quoteEscape + this.quote);
		}

		// Replace carriage return characters if the replacement string is specified
		if (this.replaceCR != null) {
			s = s.replace("\r", this.replaceCR);
		}

		// Replace newline characters if the replacement string is specified
		if (this.replaceNL != null) {
			s = s.replace("\n", this.replaceNL);
		}
		return(s);
	}


	// Formats the header value for column naming
	private String fixHeader(String v, int column) {
		// Ensure the input value is not null
		String s = (v != null) ? v : "";

		// Convert the header value to lowercase and remove leading/trailing whitespaces
		s = s.toLowerCase().trim();

		// Replace NULLSTRING with an empty string
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}

		// Remove quotes from the header value
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, "");
		}

		// Replace special characters in the header value based on the predefined mapping
		for (String[] m : this.replaceHeaderChars) {
			if (s.contains(m[0])) {
				s = s.replace(m[0], m[1]);
			}
		}

		// If the resulting header value is empty, use a default naming convention based on the column index
		if (s.length() < 1) {
			s = "c" + column;
		}
		return(s);
	}


	// Formats a number using the specified DecimalFormat and removes unnecessary trailing zeros and decimal points
	private String formatNumber(Double d, DecimalFormat decimalFormat) {
		// Format the number using the provided DecimalFormat
		String t = decimalFormat.format(d);

		// Remove trailing zeros
		t = t.replaceAll("0+$", "");

		// Remove decimal point (is integer or number without decimals)
		t = t.replaceAll("\\.$", "");

		// Replace UTF-8 minus-sign-like character at the beginning with ASCII minus sign
		return this.replaceDashToAscii(t);
	}

	// Formats a date as a string in the "yyyy-MM-dd HH:mm:ss" format
	private String formatDate(Date dt) {
		return new DateTime(dt).toString("yyyy-MM-dd HH:mm:ss");
	}





	// Converts an Excel file to a CSV file.
	// Takes input and output streams along with the file type as parameters.
	public RunStatusDto convert(InputStream in, OutputStream out, String fileType) {
		// Log the baseName for debugging purposes
		this.log("baseName: " + this.baseName);

		// Set email subject related to the baseName for error reporting
		this.mail.setSubject("Radanpito, virheitä tiedostossa: " + this.baseName);
		this.mail.setBaseName(this.baseName); 

		// Create a RunStatusDto object and set its initial status to false
		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		Workbook workbook = null;
		DataFormatter formatter = null;
		DecimalFormat decimalFormat = null;
		Sheet xlsheet = null;

		try {
			// Attempt to open the Excel workbook
			workbook = WorkbookFactory.create(in);
			formatter = new DataFormatter(true);

			// Set decimal format for numbers
			DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
			decimalFormatSymbols.setDecimalSeparator('.');
			decimalFormat = new DecimalFormat("0.0000000000", decimalFormatSymbols);
		} catch (Exception e) {
			// Log and set error details if workbook open fails
			this.log("convert(): workbook open failed: '" + e.toString() + "': '" + e.getMessage() + "'");
			result.setErrorMessage(e.toString() + ", " + e.getMessage());
			result.setStatus(false);
			return(result);
		}

		// Check if the workbook is successfully opened
		if (workbook != null) {		
			// Check and process the given sheet names
			if (this.sheetNames != null) {
				if (this.sheetNames.length == 1) {
					if (this.sheetNames[0] == null) {
						// 0: No sheet specified, convert the first sheet
						this.sheetNames = new String[1];
						this.sheetNames[0] = workbook.getSheetName(0);
					} else if (CONVERT_ALL_SHEETS.equalsIgnoreCase(this.sheetNames[0])) {
						// 1: Convert all sheets if "convert all sheets" is specified
						int sheets = workbook.getNumberOfSheets();
						this.sheetNames = new String[sheets];
						for (int i = 0; i < sheets; i++) {
							this.sheetNames[i] = workbook.getSheetName(i);
						}
					} // 2: Convert listed sheets
				} // 2: Convert listed sheets
			} else if (this.sheetNames == null) {
				// 3: == 0: No sheet specified, convert the first sheet
				this.sheetNames = new String[1];
				this.sheetNames[0] = workbook.getSheetName(0);
			}

			// Convert sheets
			this.log("Convert " + this.sheetNames.length + " sheets");
			for (int i = 0; i < this.sheetNames.length; i++) {
				String sheetName = this.sheetNames[i];
				this.log("Convert sheet = '" + sheetName + "'");
				xlsheet = workbook.getSheet(sheetName);
				if (xlsheet != null) {
					boolean useHeader = (i == 0);
					if (!this.hasHeader) useHeader = true;
					result = this.convertSheet(xlsheet, formatter, decimalFormat, useHeader, sheetName, in, out);
					if (!result.getStatus()) {
						// Error: log & exit
						this.log("Conversion error, sheet = '" + sheetName + "' : '" + result.getErrorCode() + "', '" + result.getErrorMessage() + "'. Exit");
						return(result);
					}
				} else {
					// Invalid sheet name: log & continue
					this.log("Convert sheet '" + sheetName + "': not found, continue.");

				}
			}
			this.log("Convert done, return status = " + result.getStatus());
		}

		return(result);
	}





	// Convert sheet
	public RunStatusDto convertSheet(Sheet xlsheet, DataFormatter formatter, DecimalFormat decimalFormat, boolean useHeader, String sheetName, InputStream in, OutputStream out) {
		// Create a RunStatusDto object and set its initial status to false
		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		int cols = 0;
		int col = 0;
		int rows = 0;
		int row = 0;
		String value = "";
		Row xlrow = null;
		Cell xlcell = null;

		// Set to keep track of duplicate values
		Set<String> duplicates = new HashSet<>();

		// Check if this is not a master Excel file
		if (!master){
			try{
				// Attempt to retrieve and parse the header year from the first part of cell [I8]
				String vuosiOtsikko = this.fixOutputValue(xlsheet.getRow(7).getCell(8).getStringCellValue());
				this.otsikkoVuosi = Integer.parseInt(vuosiOtsikko.split(" ")[0]);
			} catch (Exception e) {
				// Log if header year retrieval fails
				this.log("Otsikon vuoden hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
			try {
				// Attempt to retrieve and log 'teema' from cell [C5]
				this.teema = this.fixOutputValue(xlsheet.getRow(4).getCell(2).getStringCellValue());
				if (this.teema.length() > 0 ){
					this.log("Loydetty teema: " + this.teema);
				}
				else {
					this.log("Teemaa ei loydetty");
				}
			// Log if retrieval of teema fails
			} catch (Exception e) {
				this.log("Teeman hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
			try {
				// Attempt to retrieve and log the 'taustaryhman selite' from cell [B5]
				this.trSelite = this.fixOutputValue(xlsheet.getRow(4).getCell(1).getStringCellValue());
				if (this.trSelite.length() > 0 ){
					this.log("Loydetty taustaryhman selite: " + this.trSelite);
					if (this.teema.length() == 0){
						this.teema = this.trSelite;
					}
				}
				else {
					this.log("Taustaryhman selitetta ei loydetty");
				}
			} catch (Exception e) {
				// Log if retrieval of 'taustaryhman selite' fails
				this.log("Taustaryhman selitteen hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
		}
		// If it is master Excel file
		else {
			try{
				// Attempt to retrieve and parse the header year from cell [B1]
				String vuosiOtsikko = this.fixOutputValue(xlsheet.getRow(0).getCell(2).getStringCellValue());
				this.otsikkoVuosi = Integer.parseInt(vuosiOtsikko.split(" ")[0]);
			} catch (Exception e) {
				// Log if header year retrieval fails
				this.log("Otsikon vuoden hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
		}

		try{
			// Attempt to parse the year from the modifiedAt field
			this.muokkausVuosi = Integer.parseInt(this.modifiedAt.split("-")[0]);
		} catch (Exception e) {
			// Log an error if parsing the modified year fails
			this.log("Muokkausvuoden hakeminen epaonnistui.");
			this.log(e.toString() + ", " + e.getMessage());
		}

		    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		   //  Here we are attempting to place the data to correct year and season. The idea is, that if the file comes        //
		  //  in january, but the year in the header is last year, we assume it is supposed to be for last year december.     //
		 //  If the year is, for some reason next year, we assume that the data is supposet to be for next year's january.   //
		//  If we are deealing with a correction file, we ignore the year in header, since the season is given in filename  //
	   //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// Check if the header year and the specified conversion year do not match, and it's not a correction file
		if (this.otsikkoVuosi != this.year && !this.korjaustiedosto){
			this.log("Otsikossa oleva vuosi (" + this.otsikkoVuosi + ") ei tasmaa latausvuoden (" + this.year + ") kanssa.");
			
			// Check if the modification year is available
			if (this.muokkausVuosi != 0){
				// Check if the header year is before the current year
				if (this.muokkausVuosi > this.otsikkoVuosi){
					this.log("Tiedostoa on muokattu otsikkovuoden " + this.otsikkoVuosi + " jalkeen.");
					this.log("Asetetaan vuodeksi " + this.otsikkoVuosi + " ja kuukaudeksi 12");
					this.year =this.otsikkoVuosi;
					this.month = 12;
				}
				// Check if the header year is the current year
				else if (this.muokkausVuosi == this.otsikkoVuosi){
					this.log("Muokkausvuosi tasmaa otsikkovuoden kanssa. Otetaan kuukausi muokkaushetken aikaleimasta.");
					this.month = Integer.parseInt(this.modifiedAt.split("-")[1]);
				}
				// Check if the header year is after the current year
				else{
					this.log("Tiedostoa on muokattu ennen otsikkovuotta" + this.otsikkoVuosi);
					this.log("Asetetaan vuodeksi " + this.otsikkoVuosi + " ja kuukaudeksi 1");
					this.year =this.otsikkoVuosi;
					this.month = 1;
				}
			}
		}
		// Check if the header year and the specified conversion year do not match, and we are dealing with a correction file
		else if (this.otsikkoVuosi != this.year && this.korjaustiedosto){
			this.log("Otsikossa oleva vuosi (" + this.otsikkoVuosi + ") ei tasmaa korjauskauden vuoden (" + this.year + ") kanssa.");
			this.log("Kaytetaan korjauskauden vuotta (" + this.year + ")");
		}

		if (xlsheet != null) {

			int datalines = 0;

			// Skip processing if there are no data rows
			if(xlsheet.getPhysicalNumberOfRows() > 0) {
				rows = xlsheet.getLastRowNum();


				this.log("modifiedAt value is: " + this.modifiedAt);

				int emptyCount = 0;

				// Process all rows starting from the specified skipheaders row
				for(row = this.skipheaders; row <= rows; row++) {

					boolean skipEmptyRow = true;
					boolean masterIdNotFound = false;
					xlrow = xlsheet.getRow(row);
					if (xlrow != null) {
						List<String> ln = new ArrayList<String>();

						// Check if this is the first row (header row) to determine the number of columns
						if (row == this.skipheaders) {

							// If a header exists, determine the number of columns from it
							if (this.hasHeader) {
								cols = xlrow.getLastCellNum();

								// Find the first non-empty column from the end ==> each column must have a header defined
								for(col = cols; col >= 0; col--) {
									xlcell = xlrow.getCell(col);
									value = formatter.formatCellValue(xlcell);
									if (value == null) value = "";
									if (value.equalsIgnoreCase(XlsToCsvConverter.NULLSTRING)) {
										value = "";
									}
									if (!value.equals("")) {
										cols = col;
										break;
									}
								}
							} else {
								// No header, take all columns from the first read row
								cols = xlrow.getLastCellNum();
							}
							this.log("Sheet '" + sheetName + "': skip rows before header = " + this.skipheaders + ", columns = " + cols + ", rows = " + rows + ", include sheet name = " + this.includeSheetName);
						}

						// Not used in this project...
						if (this.includeSheetName) {
							// Add the sheet name as the first column
							if (this.hasHeader && (row == this.skipheaders) && useHeader) {
								// If header exists and it's the header row, use a fixed header for the sheet name
								value = this.fixHeader("SHEET_NAME", 0);
							} else {
								// Otherwise, fix the output value for the sheet name
								value = this.fixOutputValue(sheetName);
							}
							ln.add(value);
						}

						// If we are dealing with a master file, set the number of columns to 13. This allows the users to add additional columns
						// to the end of master excel without it affecting the resulting CSV...
						if (this.master){
							if (cols > 13){
								cols = 13;
							}
						}
						for(col = 0; col <= cols; col++) {
							// Iterate through each column
							xlcell = xlrow.getCell(col);
							value = "";
							if (xlcell != null) {
								// Check the cell type and handle accordingly
								if (xlcell.getCellType() == CellType.NUMERIC) {
									// Numeric or date
									double d = xlcell.getNumericCellValue();
									if (DateUtil.isCellDateFormatted(xlcell)) {
										// Date
										value = this.formatDate(xlcell.getDateCellValue());
									} else {
										// Numeric
										value = this.formatNumber(d, decimalFormat);
									}
								} else if (xlcell.getCellType() == CellType.FORMULA) {
									// Formula
									CellType t = xlcell.getCachedFormulaResultType();
									if (t == CellType.NUMERIC) {
										// Numeric result from formula
										double d = xlcell.getNumericCellValue();
										if (DateUtil.isCellDateFormatted(xlcell)) {
											// Date
											value = this.formatDate(xlcell.getDateCellValue());
										} else {
											// Numeric
											value = this.formatNumber(d, decimalFormat);
										}
									} else if (t == CellType.BOOLEAN) {
										// Boolean result from formula
										if (xlcell.getBooleanCellValue()) {
											value = "1";
										} else {
											value = "0";
										}
									} else if (t == CellType.ERROR) {
										// Error result from formula => empty value
										value = "";
									} else {
										// Other formula result
										value = xlcell.getStringCellValue();
									}

								} else {
									// Non-numeric cell types
									value = formatter.formatCellValue(xlcell);
								}

								// Perform additional value processing. We deal with non-breaking etc. weird spaces.
								value = replaceNonBreakingSpaces(value);
								value = replaceSpaces(value);
							}

							// Handle special cases based on column index and file type
							if (korjaustiedosto && !this.master && col == 5 && value.length() <= 0){
								// In correction file, ohjelmointilomake, if kohde_toimenpide is empty, set value to "REMOVED"
								value = (this.fixOutputValue("REMOVED"));
							}	
						
							if (korjaustiedosto && this.master && col == 0 && value.length() <= 0){
								// In correction file, master excel, if first column is empty, set value to "REMOVED"
								value = (this.fixOutputValue("REMOVED"));
								if (row == skipheaders){
									// If it's the header row, we set the first columns header as 'hanke'
									value = (this.fixOutputValue("hanke"));
								}
							}

							// Add the formatted value to the list
							ln.add(this.fixOutputValue(value));
							
							// Additional processing for the master sheet and specific columns
							if (this.master && col == 1){
								// For master sheet, non-empty and non-"TBA" value in column 1
								if (((this.fixOutputValue(value).length()) > 0) && !(this.fixOutputValue(value).equals("TBA"))){								
            						// Check for duplicates and handle accordingly
									skipEmptyRow = false;
									if (!duplicates.add(value)){
										// Duplicate found, log and add to error mail
										this.log("VIRHE: Rivi " + (row + 1) + ": rivi-id ( " + ln.get(col) + " ) duplikaatti rivi-id loydetty" + "\n" + "Tiedostossa: " + this.sourceKey);
										this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(col) + " ) duplikaatti rivi-id loydetty.");
									}
								}
								else{
									// Empty or "TBA" value, skip the row
									skipEmptyRow = true;
									masterIdNotFound = true;
									col = cols;
								}
							}

							if (!this.master && col == 5){
								if (this.fixOutputValue(value).length() <= 0){
									// For non-master sheet and empty kohde_toimenpide (value in column 5)
            						// Increment the count of missing kohde_toimenpide if not a correction file
									if (!korjaustiedosto){
										this.puuttuvatKohdeToimenpiteet++;
									}

								}
							}

							if (skipEmptyRow){
								// Check conditions for skipping empty rows
								if (col < (cols) && !masterIdNotFound){
									// For columns before the last one and no master id not found
									if ((this.fixOutputValue(value).length()) > 0 && !(col == 5 && value == "REMOVED")){
										// If the value is non-empty and not "REMOVED" in column 5, do not skip the row
										skipEmptyRow = false;
									}
								}
								else if (col == cols){
									// For the last column
									if (!korjaustiedosto){
										// For non-correction files
										emptyCount++;
										if (row == rows){
											// If it's the last row, log the number of removed empty rows
											this.log("Removed " + emptyCount + " empty rows.");
											if ((this.puuttuvatKohdeToimenpiteet - emptyCount) > 0){
												// If there are missing kohde_toimenpide despite removing empty rows, log a warning
												this.log("VAROITUS: kohde_toimenpide puuttui " + (this.puuttuvatKohdeToimenpiteet - emptyCount) + ":lta kantaan luetulta rivilta.");
											}
										}
									}
									else{
										// For correction files
										if (!master && this.fixOutputValue(value).length() > 0){
											// For ohjelmointilomake and non-empty values, do not skip the row
											skipEmptyRow = false;
											this.log("Poistettava rivi: ( " + (row + 1) + " ); rivi-id: ( " + this.fixOutputValue(value) + " )");
										} 
									}
								}
							}
							// For non-empty rows and the last column
							if (!skipEmptyRow && col == cols){
								
								// For the header row
								if (row == this.skipheaders){
									
									// Add fixed output values for specific columns
									ln.add(this.fixOutputValue("modified_at"));
									ln.add(this.fixOutputValue("year"));
									ln.add(this.fixOutputValue("month"));					
									ln.add(this.fixOutputValue("taustaryhma"));

									if (!this.master) {
										// Add additional columns for ohjelmointilomake sheets
										ln.add(this.fixOutputValue("tr_selite"));
									}

									ln.add(this.fixOutputValue("teema"));

									if (!this.master) {
										 // Add more columns for non-master sheets
										ln.add(this.fixOutputValue("teema_selite"));
										ln.add(this.fixOutputValue("projektinumero"));
									}

									if (this.master){
										// Add columns specific to master excel
										ln.add(this.fixOutputValue("rivi_id"));
										ln.add(this.fixOutputValue("taso"));
									}

									if (!this.master) {
										// Add more columns for non-master sheets
										ln.add(this.fixOutputValue("tiedostonimi"));
									}
									
								}
								else{
									// For non-empty rows and columns other than the last

									// Add fixed output values for specific columns
									ln.add(this.fixOutputValue(this.modifiedAt));
									ln.add(this.fixOutputValue(String.valueOf(this.year)));
									ln.add(this.fixOutputValue(String.valueOf(this.month)));

									if (!this.master) {
										// For ohjelmointilomake

       									// Check for duplicate values and log errors
										if (!duplicates.add(value) && value.length() > 0){
											this.log("VIRHE: Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ) duplikaatti rivi-id loydetty" + "\n" + "Tiedostossa: " + this.sourceKey);
											this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ) duplikaatti rivi-id löydetty.");
										}

										// Check for missing or empty values and log errors
										if (value == null || value.length() <= 0){
											this.log("VIRHE: Rivi-id puuttuu rivilta " + (row + 1) + "\n" + "Tiedostossa: " + this.sourceKey);
											this.mail.add("- Rivi " + (row + 1) + ": rivi-id puuttuu.");
											ln.add(this.fixOutputValue(""));
											ln.add(this.fixOutputValue(this.trSelite));
											ln.add(this.fixOutputValue(""));
											ln.add(this.fixOutputValue(this.teema));
										}
										else {
											// Split and process the rivi-id value
											String[] tr = this.fixOutputValue(value).split("-");
											if (tr.length >= 2){
												ln.add(this.fixOutputValue(tr[0]));
												ln.add(this.fixOutputValue(this.trSelite));
												ln.add(this.fixOutputValue(tr[0] + "-" + tr[1]));
												ln.add(this.fixOutputValue(this.teema));
											}
											else{
												// Log an error for incorrect rivi-id format
												this.log("VIRHE: Rivi-id on vaarassa muodossa, teemaa ja taustaryhmaa ei saada johdettua" + "\n" + "Tiedostossa: " + this.sourceKey);
												this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ): Rivi-id on väärässä muodossa.");
												ln.add(this.fixOutputValue(""));
												ln.add(this.fixOutputValue(this.trSelite));
												ln.add(this.fixOutputValue(""));
												ln.add(this.fixOutputValue(this.teema));
											}
										}

										// Sampo toimenpide PR-numero
										if (ln.get(30).length() > 0){
											ln.add(ln.get(30));
										}
										// Sampo projekti PR-numero
										else if (ln.get(29).length() > 0){
											ln.add(ln.get(29));
										}
										// Sampo hanke PR-numero
										else if (ln.get(28).length() > 0){
											ln.add(ln.get(28));
										}
										// Project number not found
										else{
											ln.add(this.fixOutputValue(""));
										}
									}
									if (this.master) {
										// For master sheets

										// Split rivi-id value
										String[] tr = ln.get(1).split("-");

										// Process based on the length of the split array
										if (tr.length == 0){
											ln.add("");
											ln.add("");
											ln.add("");
											ln.add("N/A");
										}

										if (tr.length == 1){
											ln.add(this.fixOutputValue(tr[0]));	
											ln.add("");
											ln.add("");
											ln.add(this.fixOutputValue("Taustaryhmä"));
										}

										if (tr.length == 2){
											ln.add(this.fixOutputValue(tr[0]));
											ln.add(this.fixOutputValue(tr[0] + "-" + tr[1]));
											ln.add("");
											ln.add(this.fixOutputValue("Teema"));
										}

										if (tr.length == 3){
											ln.add(this.fixOutputValue(tr[0]));
											ln.add(this.fixOutputValue(tr[0] + "-" + tr[1]));
											ln.add(this.fixOutputValue(tr[0] + "-" + tr[1] + "-" + tr[2]));
											ln.add(this.fixOutputValue("Rivi"));
										}
										
									}

									// For ohjelmointilomake we add tiedostonimi
									if (!this.master) {
										ln.add(this.fixOutputValue(this.baseName));
									}

								}

								// Additional processing for the last row

								if (row == rows){
									if (emptyCount > 0){
										this.log("Poistettiin " + emptyCount + " tyhjaa rivia.");
									}
									if (this.puuttuvatKohdeToimenpiteet > emptyCount){
										this.log("VAROITUS: kohde_toimenpide puuttui " + (this.puuttuvatKohdeToimenpiteet - emptyCount) + ":lta kantaan luetulta rivilta.");
									}
								}

							}
						}

						

						int c = 0;
						String s = "";

						if ((this.hasHeader) && (row == this.skipheaders)) {
							// Writing the header
							if (useHeader) {
								for (String v : ln) {
									if (c > 0) {
										s += this.delimiter;
									}
									s += this.fixHeader(v, c);
									c++;
								}
							}
						} else if (!skipEmptyRow && !masterIdNotFound) {
							// Writing the data
							for (String v : ln) {
								if (c > 0) {
									s += this.delimiter;
								}
								if (this.quote != null) {
									s += this.quote;
								}
								if (this.trimData) {
									//s += v.replaceAll("^ +| +$|( )+", "$1"); // huomioi extra spacet keskella
									s += v.trim();
								} else {
									s += v;
								}
								if (this.quote != null) {
									s += this.quote;
								}
								c = 1;
							}
						}

						if (this.hasHeader && (row == this.skipheaders) && (!useHeader)) {
							// Skip for headers without useHeader

						} else if (!skipEmptyRow) {
							// Writing the end-of-line and flushing to the output stream
							s += this.eol;
							try {
								out.write(s.getBytes(this.charSet));
								datalines++;
							} catch (Exception e) {
								result.setErrorMessage(e.toString() + ", " + e.getMessage());
								result.setStatus(false);
								return(result);
							}
						}

					}
				}
			}

			this.log("Sheet '" + sheetName + "': data lines written: " + datalines);
			result.setStatus(true);
		}

		if (this.mail.getMessage().length() > 0){
			// If there are error messages, send an error email
			this.log("Lähetetään virhesahkoposti...");
			this.mail.add(
				"\n\nTarvittaessa ota yhteyttä:" +
				"\nKanta/lataukset: vaylavirasto.ade@cgi.com" +
				"\nValvonta: vayla.apk@cgi.com" +
				"\n\n Ystävällisin terveisin,\n" + 
				" CGI Suomi - Radanpidon raportointi"
				);
			if (alarmTopic.length() > 0){
				this.mail.sendSns(this.alarmTopic);
			}
		}

		return result;
	}

}