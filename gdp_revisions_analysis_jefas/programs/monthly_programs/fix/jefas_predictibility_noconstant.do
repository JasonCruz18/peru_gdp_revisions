/********************
Encompassing Test
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: jefas_encompassing.do
		** 	First Created: 03/20/25
		** 	Last Updated:  03/21/25
			
	***
	** Just click on the "Run (do)" button, the code will do the rest for you.
	***

	
	
	/*----------------------
	Initial script configuration
	-----------------------*/

	cls 					// Clears the screen.
	clear all 				// Frees all memory.
	version 				// Displays the software version.
		
	set more off 			// Turns off pagination for output.
	cap set maxvar 12000	// Sets the maximum number of variables to 12000.
	program drop _all 		// Deletes all user-defined programs.
			
	capture log close 		// Closes the log file if open.
				
	pause on 				// Enables pauses in programs.
	set varabbrev off 		// Turns off variable abbreviation.
			
	//log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

	

	/*----------------------
	Defining workspace path
	------------------------*/
	
	di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"
		
		
		
	/*----------------------
	Setting folders to save outputs
	------------------------*/
		
	shell mkdir "output" 			// Creates folder to save outputs.
	//shell mkdir "output/charts" 	// Creates folder to save charts.
	shell mkdir "output/tables" 	// Creates folder to save tables.
	//shell mkdir "output/data" 		// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/charts"	// Use to export charts.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC dataset and
	save temp
	-----------------------*/
		
		
	odbc load, exec("select * from e_gdp_monthly_releases") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change the dataset loaded from SQL as preferred. 
		
	
	save gdp_releases, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(GDP releases)
	-----------------------*/

		
	use gdp_releases, clear
	
	
		* Remove the current definitive value (most recent release) for each target period and sector
		
		drop *_most_recent
		
		
		* Remove columns for h>12
		
		ds *_release_*
		
		foreach var in `r(varlist)' {
			if regexm("`var'", "([0-9]+)$") { // Extract the number at the end of the variable name
				local num = regexs(1)  // We use regexs(1) to capture the number

				if real("`num'") > 12 { // Check if "num" is greater than 12
					drop `var'
				}
			}
		}
				
				
		* Redefine the 12th release as the definitive value of GDP growth
		
		rename gdp_release_12 gdp_most_recent
	
	
		* Format the date variable
		
		replace vintages_date = mofd(dofc(vintages_date))
		format vintages_date %tm
		
		
		* Set vintages_date as first column
		
		order vintages_date
		
		
		* Sort by vintages_date
		
		sort vintages_date
		
		
		* Keep obs in specific date range
		
		keep if vintages_date > tm(1992m12) & vintages_date < tm(2023m11)
		
	
	save gdp_releases_cleaned, replace
	
	
	
	/*----------------------
	Compute ongoing
	revisions (r)
	-----------------------*/
	
	
	use gdp_releases_cleaned, clear
	
	
		* Generate ongoing revisions for each horizon and sector
		
		forval i = 2/11 {
			gen r_`i'_gdp = gdp_release_`i' - gdp_release_`=`i'-1'
		}
		
		
		* Compute final revision (12th horizon)
		
		gen r_12_gdp = gdp_most_recent - gdp_release_11				

	
	save r_gdp_releases, replace
	
	
	
	/*----------------------
	Compute prediction
	errors (e)
	-----------------------*/

	
	use r_gdp_releases, clear
	
	
		* Generate forecast error for each horizon and sector
		
		forval i = 1/11 {
			gen e_`i'_gdp = gdp_most_recent - gdp_release_`i'
		}
		
	
	save r_e_gdp_releases, replace
	
	
	
	/*----------------------
	r & e: Encompassing Test
	________________________
	Paper and presentation
	version
	-----------------------*/

	
	use r_e_gdp_releases, clear
	
	
		* Keep common obs

		** Set common information using regression for model III (H1) to keep if !missing(residuals)

		qui {
			reg e_11_gdp r_11_gdp, noconstant
			predict residuals_aux, resid  // Generate the regression residuals.
		}

		keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.

		qui drop residuals_aux
	
		* Create a new frame named `r_e_encompassing` to store regression results
		
		frame create r_e_predictibility str32 variable int n str32 coef_1
		
		
		* Loop through variables e_`i'_gdp where `i' ranges from 1 to 11
		
		forval i = 2/11 {
			
			capture confirm variable e_`i'_gdp
			
			if !_rc {
				
				capture {
					
					quietly count if !missing(e_`i'_gdp)
					if r(N) < 5 continue  // Salta si hay menos de 5 observaciones
					
					reg e_`i'_gdp r_`i'_gdp, noconstant
					
					if _rc == 2001 {
						di in red "Insufficient observations for e_`i'_gdp"
						continue
					}
					
					matrix M = r(table)
					
					summarize e_`i'_gdp, detail
					local n = r(N)
					
					local coef_1 = M[1, 1] // r_`i'_gdp
					local se_1 = M[2, 1]
					local pvalue_1 = M[4, 1] // constant p-value
					
					if `pvalue_1' < 0.01 {
						local coef_1 = string(`coef_1', "%9.2f") + "***"
					}
					else if `pvalue_1' < 0.05 {
						local coef_1 = string(`coef_1', "%9.2f") + "**"
					}
					else if `pvalue_1' < 0.10 {
						local coef_1 = string(`coef_1', "%9.2f") + "*"
					}
					else {
						local coef_1 = string(`coef_1', "%9.2f")
					}
					
					*** Append standard error in parentheses to coef
					local coef_1 = "`coef_1'" + char(10) + "(" + string(`se_1', "%9.2f") + ")"
					
					frame post r_e_predictibility ("e_`i'_gdp") (`n') ("`coef_1'")
				}
			}
			
			else {
				di in yellow "Variable e_`i'_gdp does not exist"
			}
		}

		frame change r_e_predictibility

		list variable n coef_1, noobs clean
				
				
		* Rename vars
		
		rename variable h
		rename coef_1 Beta
		
		
		* Order vars
		
		order h n Beta
	
		
		* Export to excel file
		
		export excel using "$tables_folder/gdp_predictibility_noconstant.xlsx", ///
    firstrow(variable) replace
					
		
	
	/*----------------------
	Drop aux data and tables
	-----------------------*/	

	* List all .dta, .txt and .tex files in the current directory and store in a local macro
	
	local dta_files : dir . files "*.dta"
	//local txt_files : dir . files "*.txt"
	//local tex_files : dir . files "*.tex"

	
	* Iterate over each .dta file and delete it
	
	foreach file of local dta_files {
		erase "`file'"
	}	

	
	* Iterate over each .txt file and delete it
	
	//foreach file of local txt_files {
	//	erase "`file'"
	//}	
	
	
	* Iterate over each .tex file and delete it
	
	//foreach file of local tex_files {
	//	erase "`file'"
	//}	
	
	