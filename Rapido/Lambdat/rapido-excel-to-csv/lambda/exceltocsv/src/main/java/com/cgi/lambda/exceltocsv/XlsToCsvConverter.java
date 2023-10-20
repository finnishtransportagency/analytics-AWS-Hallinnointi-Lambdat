package com.cgi.lambda.exceltocsv;


import java.util.ArrayList;

// import java.util.Locale;
// import java.util.Map;
import java.util.Set;
import java.util.Date;
// import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.io.InputStream;
import java.io.OutputStream;
// import java.io.File;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

// import org.apache.poi.xssf.usermodel.XSSFWorkbook;
// import org.apache.poi.hssf.usermodel.HSSFWorkbook;
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
 * Editoitu 2023-05-24
 * 
 * @author isto.saarinen, edit. arto.lautamo
 *
 *
 *
 *
 *
 */
public class XlsToCsvConverter {

	private static final String CONVERT_ALL_SHEETS = "*";

	private static final String NULLSTRING = "null";
	private boolean hasHeader = false;
	private String replaceCR = "";
	private String replaceNL = " ";
	private String delimiter = ";";
	private String[] sheetNames = null;
	private boolean includeSheetName = false;
	private String charSet = "UTF-8";
	private String eol = System.lineSeparator();
	private String quote = "\"";
	private String quoteEscape = "\"";
	private String[][] replaceHeaderChars = { {"ä","a"},  {"å","a"}, {"ö","o"}, {" ","_"} };
	private boolean trimData = false;
	private int skipheaders = 0;
	private String modifiedAt = null;
	private String teema = "";	
	private String trSelite = "";	
	private boolean master = false;
	private int year = 0;
	private int month = 0;
	private String sourceKey = "";
	private String baseName = "";
	private int otsikkoVuosi = 0;
	private int muokkausVuosi = 0;
	private boolean korjaustiedosto = false;
	private int puuttuvatKohdeToimenpiteet = 0;

	private SimpleLogger logger = null;

	private ErrorMail mail = new ErrorMail();

	private String alarmTopic = "";


	private void log(String s) {
		if (this.logger != null) {
			this.logger.log(s);
		}
	}


	// Alkaako merkkijono jollakin UTF-8 miinus- merkillä ?
	// https://jkorpela.fi/dashes.html
	private boolean startsWithSomeDash(String s) {
		boolean b = false;
		int[] dashList = {
				45, 6150, 8208, 8209, 8210, 8211, 8212, 8213, 8315, 8331, 8722,  11834, 11835, 65112, 65123, 65293
		};
		int c = (int)s.charAt(0);
		for (int i : dashList) {
			if (c == i) {
				b = true;
			}
		}
		return(b);
	}

	// Viivojen korvaus ascii miinuksella
	private String replaceDashToAscii(String s) {
		boolean b = this.startsWithSomeDash(s);
		String r = s;
		if (b) {
			r = ((char)45) + s.substring(1);
		}
		return r;
	}

	private String replaceNonBreakingSpaces(String s) {
		if (s == null) return(null);
		s.replaceAll("\u00A0", " ");
		s.replaceAll("\u2007", " ");
		s.replaceAll("\u202F", " ");
		return s;
	}

	public static String replaceSpaces(String s) {
		if (s == null) return(null);
		int[] spaceList = {
				160, 5760, 6158, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8203, 8239, 8287, 12288, 65279
		};
		for(int c: spaceList) {
			s = s.replace((char)c, ' ');
		}
		return(s);
	}

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

	// Tulostettavan tiedon muotoilut
	private String fixOutputValue(String v) {
		String s = (v != null) ? v : "";
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, this.quoteEscape + this.quote);
		}
		if (this.replaceCR != null) {
			s = s.replace("\r", this.replaceCR);
		}
		if (this.replaceNL != null) {
			s = s.replace("\n", this.replaceNL);
		}
		return(s);
	}


	// Otsikon muotoilut
	private String fixHeader(String v, int column) {
		String s = (v != null) ? v : "";
		s = s.toLowerCase().trim();
		if (s.equals(XlsToCsvConverter.NULLSTRING )) {
			s = "";
		}
		if (s.contains(this.quote)) {
			s = s.replace(this.quote, "");
		}
		for (String[] m : this.replaceHeaderChars) {
			if (s.contains(m[0])) {
				s = s.replace(m[0], m[1]);
			}
		}
		if (s.length() < 1) {
			s = "c" + column;
		}
		return(s);
	}


	// Numeron muotoilu
	private String formatNumber(Double d, DecimalFormat decimalFormat) {
		String t = decimalFormat.format(d);
		// Remove trailing zeros
		t = t.replaceAll("0+$", "");
		// Remove decimal point (is integer or number without decimals)
		t = t.replaceAll("\\.$", "");
		return this.replaceDashToAscii(t);
	}

	private String formatDate(Date dt) {
		return new DateTime(dt).toString("yyyy-MM-dd HH:mm:ss");
	}





	// Muunna. Kutsuja antaa in ja out streamit
	public RunStatusDto convert(InputStream in, OutputStream out, String fileType /*, File excelFile*/) {
		this.log("baseName: " + this.baseName);
		this.log("baseName: " + this.baseName);
		this.mail.setSubject("Radanpito, virheitä tiedostossa: " + this.baseName);
		this.mail.setBaseName(this.baseName); 

		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		Workbook workbook = null;
		DataFormatter formatter = null;
		DecimalFormat decimalFormat = null;
		Sheet xlsheet = null;

		try {

			workbook = WorkbookFactory.create(in);
			formatter = new DataFormatter(true);
			DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
			decimalFormatSymbols.setDecimalSeparator('.');
			decimalFormat = new DecimalFormat("0.0000000000", decimalFormatSymbols);
		} catch (Exception e) {
			this.log("convert(): workbook open failed: '" + e.toString() + "': '" + e.getMessage() + "'");
			result.setErrorMessage(e.toString() + ", " + e.getMessage());
			result.setStatus(false);
			return(result);
		}
		if (workbook != null) {

			
			// Annettujen lehtien tarkastus
			if (this.sheetNames != null) {
				if (this.sheetNames.length == 1) {
					if (this.sheetNames[0] == null) {
						// 0: Ei annettua lehteä -> muunnetaan ensimmäinen lehti
						this.sheetNames = new String[1];
						this.sheetNames[0] = workbook.getSheetName(0);
					} else if (CONVERT_ALL_SHEETS.equalsIgnoreCase(this.sheetNames[0])) {
						// 1: Annettu lehti "convert all sheets" -> muunnetaan kaikki lehdet samaan kohteeseen.
						int sheets = workbook.getNumberOfSheets();
						this.sheetNames = new String[sheets];
						for (int i = 0; i < sheets; i++) {
							this.sheetNames[i] = workbook.getSheetName(i);
						}
					} // 2: Muunnetaan listatut lehdet
				} // 2: Muunnetaan listatut lehdet
			} else if (this.sheetNames == null) {
				// 3: == 0: Ei annettua lehteä -> muunnetaan ensimmäinen lehti
				this.sheetNames = new String[1];
				this.sheetNames[0] = workbook.getSheetName(0);
			}
			
				/*
			int sheets = workbook.getNumberOfSheets();
			this.sheetNames = new String[sheets];
			for (int i = 0; i < sheets; i++) {
				this.sheetNames[i] = workbook.getSheetName(i);
			}	
				*/

			// Muunnetaan lehdet
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





	// Muunna lehti
	public RunStatusDto convertSheet(Sheet xlsheet, DataFormatter formatter, DecimalFormat decimalFormat, boolean useHeader, String sheetName, InputStream in, OutputStream out) {
		RunStatusDto result = new RunStatusDto();
		result.setStatus(false);
		int cols = 0;
		int col = 0;
		int rows = 0;
		int row = 0;
		String value = "";
		Row xlrow = null;
		Cell xlcell = null;
		Set<String> duplicates = new HashSet<>();

		if (!master){
			try{
				String vuosiOtsikko = this.fixOutputValue(xlsheet.getRow(7).getCell(8).getStringCellValue());
				this.otsikkoVuosi = Integer.parseInt(vuosiOtsikko.split(" ")[0]);
			} catch (Exception e) {
				this.log("Otsikon vuoden hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
			try {
				this.teema = this.fixOutputValue(xlsheet.getRow(4).getCell(2).getStringCellValue());
				if (this.teema.length() > 0 ){
					this.log("Loydetty teema: " + this.teema);
				}
				else {
					this.log("Teemaa ei loydetty");
				}
			} catch (Exception e) {
				this.log("Teeman hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
			try {
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
				this.log("Taustaryhman selitteen hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
		}
		else {
			try{
				String vuosiOtsikko = this.fixOutputValue(xlsheet.getRow(0).getCell(2).getStringCellValue());
				this.otsikkoVuosi = Integer.parseInt(vuosiOtsikko.split(" ")[0]);
			} catch (Exception e) {
				this.log("Otsikon vuoden hakeminen epaonnistui.");
				this.log(e.toString() + ", " + e.getMessage());
			}
		}

		try{
			this.muokkausVuosi = Integer.parseInt(this.modifiedAt.split("-")[0]);
		} catch (Exception e) {
			this.log("Muokkausvuoden hakeminen epaonnistui.");
			this.log(e.toString() + ", " + e.getMessage());
		}

		if (this.otsikkoVuosi != this.year && !this.korjaustiedosto){
			this.log("Otsikossa oleva vuosi (" + this.otsikkoVuosi + ") ei tasmaa latausvuoden (" + this.year + ") kanssa.");
			
			if (this.muokkausVuosi != 0){
				if (this.muokkausVuosi > this.otsikkoVuosi){
					this.log("Tiedostoa on muokattu otsikkovuoden " + this.otsikkoVuosi + " jalkeen.");
					this.log("Asetetaan vuodeksi " + this.otsikkoVuosi + " ja kuukaudeksi 12");
					this.year =this.otsikkoVuosi;
					this.month = 12;
				}
				else if (this.muokkausVuosi == this.otsikkoVuosi){
					this.log("Muokkausvuosi tasmaa otsikkovuoden kanssa. Otetaan kuukausi muokkaushetken aikaleimasta.");
					this.month = Integer.parseInt(this.modifiedAt.split("-")[1]);
				}
				else{
					this.log("Tiedostoa on muokattu ennen otsikkovuotta" + this.otsikkoVuosi);
					this.log("Asetetaan vuodeksi " + this.otsikkoVuosi + " ja kuukaudeksi 1");
					this.year =this.otsikkoVuosi;
					this.month = 1;
				}
			}
		}
		else if (this.otsikkoVuosi != this.year && this.korjaustiedosto){
			this.log("Otsikossa oleva vuosi (" + this.otsikkoVuosi + ") ei tasmaa korjauskauden vuoden (" + this.year + ") kanssa.");
			this.log("Kaytetaan korjauskauden vuotta (" + this.year + ")");
		}

		if (xlsheet != null) {

			int datalines = 0;

			// ohitetaan jos ei datarivejä
			if(xlsheet.getPhysicalNumberOfRows() > 0) {
				rows = xlsheet.getLastRowNum();


				this.log("modifiedAt value is: " + this.modifiedAt);

				int emptyCount = 0;

				// Käsitellään kaikki rivit annetusta eteenpäin
				for(row = this.skipheaders; row <= rows; row++) {

					// this.log("Kasitellaan rivi: " + row);
					boolean skipEmptyRow = true;
					boolean masterIdNotFound = false;
					xlrow = xlsheet.getRow(row);
					if (xlrow != null) {
						List<String> ln = new ArrayList<String>();

						if (row == this.skipheaders) {

							// Jos otsikko on olemassa, otetaan sarakemäärä siitä
							if (this.hasHeader) {
								cols = xlrow.getLastCellNum();
								/*
								if (this.modifiedAt!=null){
									xlcell = xlrow.createCell(cols);
									xlcell.setCellValue("modifiedAt");
									this.log("Viimeinen sarake - modifiedAt");
									cols = xlrow.getLastCellNum();
									this.log("Sarakkeiden maara: " + cols);
								}
								*/
								// Haetaan lopusta ensimmäinen ei- tyhjä sarake ==>> joka sarakkeella pitää olla otsikko määritettynä
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
								// Ei otsikkoa, otetaan kaikki sarakkeet ensimmäiseltä luettavalta riviltä
								cols = xlrow.getLastCellNum();
							}
							this.log("Sheet '" + sheetName + "': skip rows before header = " + this.skipheaders + ", columns = " + cols + ", rows = " + rows + ", include sheet name = " + this.includeSheetName);
						}

						if (this.includeSheetName) {
							// Lisätään lehden nimi ensimmäiseksi sarakkeeksi
							if (this.hasHeader && (row == this.skipheaders) && useHeader) {
								value = this.fixHeader("SHEET_NAME", 0);
							} else {
								value = this.fixOutputValue(sheetName);
							}
							ln.add(value);
						}

						// Muotoillaan sarakkeen data
						// this.log("sarakkeiden maara: " + cols);
						for(col = 0; col <= cols; col++) {
							// this.log("Kasitellaan sarake: " + col);
							xlcell = xlrow.getCell(col);
							value = "";
							if (xlcell != null) {

								if (xlcell.getCellType() == CellType.NUMERIC) {
									// Numero tai pvm
									double d = xlcell.getNumericCellValue();
									if (DateUtil.isCellDateFormatted(xlcell)) {
										// Pvm
										value = this.formatDate(xlcell.getDateCellValue());
									} else {
										// Numero
										value = this.formatNumber(d, decimalFormat);
									}
								} else if (xlcell.getCellType() == CellType.FORMULA) {
									// Kaava
									CellType t = xlcell.getCachedFormulaResultType();
									if (t == CellType.NUMERIC) {
										double d = xlcell.getNumericCellValue();
										if (DateUtil.isCellDateFormatted(xlcell)) {
											// Pvm
											value = this.formatDate(xlcell.getDateCellValue());
										} else {
											// Numero
											value = this.formatNumber(d, decimalFormat);
										}
									} else if (t == CellType.BOOLEAN) {
										//
										if (xlcell.getBooleanCellValue()) {
											value = "1";
										} else {
											value = "0";
										}
									} else if (t == CellType.ERROR) {
										// Virhe => tyhjä arvo
										value = "";
									} else {
										value = xlcell.getStringCellValue();
									}

								} else {
									value = formatter.formatCellValue(xlcell);
								}

								value = replaceNonBreakingSpaces(value);
								value = replaceSpaces(value);
								/* 
								if (value == "" && xlcell.getColumnIndex() == cols && this.modifiedAt != null){
									this.log("Viimeinen sarake, laitetaan arvoksi: " + this.modifiedAt);
									value = this.modifiedAt;
								}
								*/

							}
							/*
							else if (xlcell == null && col == cols && this.modifiedAt != null){
								xlcell = xlrow.createCell(col);
								xlcell.setCellValue(this.modifiedAt);
							}
							*/
							if (korjaustiedosto && !this.master && col == 5 && value.length() <= 0){
								value = (this.fixOutputValue("REMOVED"));
								// skipEmptyRow = false;
								// this.log("Poistettava rivi: " + (row + 1));
							}	
						
							if (korjaustiedosto && this.master && col == 0 && value.length() <= 0){
								value = (this.fixOutputValue("REMOVED"));
								if (row == skipheaders){
									value = (this.fixOutputValue("hanke"));
								}
							}

							ln.add(this.fixOutputValue(value));
							

							if (this.master && col == 1){
								if (((this.fixOutputValue(value).length()) > 0) && !(this.fixOutputValue(value).equals("TBA"))){
									skipEmptyRow = false;
									if (!duplicates.add(value)){
										this.log("VIRHE: Rivi " + (row + 1) + ": rivi-id ( " + ln.get(col) + " ) duplikaatti rivi-id loydetty" + "\n" + "Tiedostossa: " + this.sourceKey);
										this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(col) + " ) duplikaatti rivi-id loydetty.");
									}
								}
								else{
									skipEmptyRow = true;
									masterIdNotFound = true;
									col = cols;
								}
							}

							if (!this.master && col == 5){
								if (this.fixOutputValue(value).length() <= 0){
									// if (this.puuttuvatKohdeToimenpiteet < 5){
									// 	this.log("VAROITUS: Rivi " + (row + 1) + ": kohde_toimenpide puuttuu." + "\n" + "Tiedostossa: " + this.sourceKey);
									// }
									if (!korjaustiedosto){
										this.puuttuvatKohdeToimenpiteet++;
									}
									// skipEmptyRow = true;

								}
							}

							if (skipEmptyRow){
								if (col < (cols) && !masterIdNotFound){
									if ((this.fixOutputValue(value).length()) > 0 && !(col == 5 && value == "REMOVED")){
										skipEmptyRow = false;
										//this.log("Row: " + row + ", Column: " + col);
										//this.log("Found value: " + fixOutputValue(value) + ", row is not empty");
									}
								}
								else if (col == cols){
									if (!korjaustiedosto){
										emptyCount++;
										if (row == rows){
											this.log("Removed " + emptyCount + " empty rows.");
											this.log("VAROITUS: kohde_toimenpide puuttui " + (this.puuttuvatKohdeToimenpiteet - emptyCount) + ":lta kantaan luetulta rivilta.");
										}
									}
									else{
										if (!master && this.fixOutputValue(value).length() > 0){
											skipEmptyRow = false;
											this.log("Poistettava rivi: ( " + (row + 1) + " ); rivi-id: ( " + this.fixOutputValue(value) + " )");
										} 
									}
								}

								//else if (skipEmptyRow){
								//	this.log("tyhja rivi: " + xlcell.getRowIndex());
								//}
							}
							if (!skipEmptyRow && col == cols){
								if (row == this.skipheaders){
									ln.add(this.fixOutputValue("modified_at"));
									ln.add(this.fixOutputValue("year"));
									ln.add(this.fixOutputValue("month"));
						
									ln.add(this.fixOutputValue("taustaryhma"));

									if (!this.master) {
										ln.add(this.fixOutputValue("tr_selite"));
									}

									ln.add(this.fixOutputValue("teema"));

									if (!this.master) {
										ln.add(this.fixOutputValue("teema_selite"));
										ln.add(this.fixOutputValue("projektinumero"));
									}

									if (this.master){
										ln.add(this.fixOutputValue("rivi_id"));
										ln.add(this.fixOutputValue("taso"));
									}

									if (!this.master) {
										ln.add(this.fixOutputValue("tiedostonimi"));
									}
									
								}
								else{
									ln.add(this.fixOutputValue(this.modifiedAt));
									//ln.add(this.fixOutputValue((this.modifiedAt).split("-")[0]));
									ln.add(this.fixOutputValue(String.valueOf(this.year)));
									//ln.add(this.fixOutputValue((this.modifiedAt).split("-")[1]));
									ln.add(this.fixOutputValue(String.valueOf(this.month)));
									if (!this.master) {
										if (!duplicates.add(value) && !(korjaustiedosto && value.length() <= 0)){
											this.log("VIRHE: Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ) duplikaatti rivi-id loydetty" + "\n" + "Tiedostossa: " + this.sourceKey);
											this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ) duplikaatti rivi-id löydetty.");
										}
										if (value == null || value.length() <= 0){
											this.log("VIRHE: Rivi-id puuttuu rivilta " + (row + 1) + "\n" + "Tiedostossa: " + this.sourceKey);
											this.mail.add("- Rivi " + (row + 1) + ": rivi-id puuttuu.");
											ln.add(this.fixOutputValue(""));
											ln.add(this.fixOutputValue(this.trSelite));
											ln.add(this.fixOutputValue(""));
											ln.add(this.fixOutputValue(this.teema));
										}
										else {
											String[] tr = this.fixOutputValue(value).split("-");
											if (tr.length >= 2){
												ln.add(this.fixOutputValue(tr[0]));
												ln.add(this.fixOutputValue(this.trSelite));
												ln.add(this.fixOutputValue(tr[0] + "-" + tr[1]));
												ln.add(this.fixOutputValue(this.teema));
											}
											else{
												this.log("VIRHE: Rivi-id on vaarassa muodossa, teemaa ja taustaryhmaa ei saada johdettua" + "\n" + "Tiedostossa: " + this.sourceKey);
												this.mail.add("- Rivi " + (row + 1) + ": rivi-id ( " + ln.get(cols) + " ): Rivi-id on väärässä muodossa.");
												ln.add(this.fixOutputValue(""));
												ln.add(this.fixOutputValue(this.trSelite));
												ln.add(this.fixOutputValue(""));
												ln.add(this.fixOutputValue(this.teema));
											}
										}
										//ln.add(this.fixOutputValue(ln.get(cols).substring(0, ln.get(cols).lastIndexOf("-"))));
										//ln.add(this.fixOutputValue(this.teema));
										if (ln.get(30).length() > 0){
											ln.add(ln.get(30));
										}
										else if (ln.get(29).length() > 0){
											ln.add(ln.get(29));
										}
										else if (ln.get(28).length() > 0){
											ln.add(ln.get(28));
										}
										else{
											ln.add(this.fixOutputValue(""));
										}
									}
									if (this.master) {
										String[] tr = ln.get(1).split("-");

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
									if (!this.master) {
										ln.add(this.fixOutputValue(this.baseName));
									}

								}

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
							// Otsikon kirjoitus
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
							// Datan kirjoitus
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
							// Ohitetaan otsikko
							//logger.log("Sheet '" + sheetName + "': skip data line " + (row + 1) + " = [header]");
						} else if (!skipEmptyRow) {
							//logger.log("Sheet '" + sheetName + "': write data line " + (row + 1) + " = [data]");
							//logger.log("Sheet '" + sheetName + "': write data line " + (row + 1) + " = [" + s.toString() + "]");
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
			this.log("Lähetetään virhesahkoposti...");
			this.mail.add(
				"\n\nTarvittaessa ota yhteyttä:" +
				"\nKanta/lataukset: vaylavirasto.ade@cgi.com" +
				"\nValvonta: vayla.apk@cgi.com" +
				"\n\n Ystävällisin terveisin,\n" + 
				" CGI Suomi - Radanpidon raportointi"
				);
			//this.mail.sendMail();
			if (alarmTopic.length() > 0){
				this.mail.sendSns(this.alarmTopic);
			}
		}

		return result;
	}


}