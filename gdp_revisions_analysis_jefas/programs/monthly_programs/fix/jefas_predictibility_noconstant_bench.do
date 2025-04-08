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
	
	
	odbc load, exec("select * from e_sectorial_gdp_monthly_releases_seasonal_dummies") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save gdp_bench_e, replace
	
	
	
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
		
		keep if vintages_date > tm(2000m12) & vintages_date < tm(2023m11)
		
	
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
	On-the-fly data cleaning
	(GDP bench e)
	-----------------------*/

		
	use gdp_bench_e, clear


		* Keep ongoing revisions for global GDP only
		
		keep vintages_date gdp_release_*
				
		
		* Remove columns for h>12
		
		ds gdp_release_* // Required to use `r(varlist)' below
		
		foreach var in `r(varlist)' {
			// Extraer el número al final del nombre de la variable
			if regexm("`var'", "gdp_release_([0-9]+)") {
				local num = regexs(1)  // Usamos regexs(1) para capturar el número

				// Verificar si el número es mayor a 12
				if real("`num'") > 12 {
					drop `var'
				}
			}
		}
				
		
		* Add the suffix "dummy" in each column
				
		foreach var of varlist * {
			if "`var'" != "vintages_date" {
				rename `var' `var'_dummy
			}
		}
					
		
		* Format all vars from double to int 
		
		ds vintages_date, not  // Captura todas las variables excepto vintages_date
		recast int `r(varlist)', force
		
		
		* Format the date variable
		
		replace vintages_date = mofd(dofc(vintages_date))
		format vintages_date %tm
		
		
		* Set vintages_monthly as first column
		
		order vintages_date
		
		
		* Sort by vintages_monthly
		
		sort vintages_date
		
		
		*** This is a provisional
		
		//drop if vintages_monthly == tm(2000m4) | vintages_monthly == tm(2013m12)
	
	
		* Keep obs in specific date range
		
		keep if vintages_date > tm(2000m12) & vintages_date < tm(2023m11)
		
	
	save gdp_bench_e_cleaned, replace
	
	
	
	/*----------------------
	Compute prediction
	errors (bench-r)
	-----------------------*/

	
	use gdp_bench_e_cleaned, clear
	
	
		* Generate forecast error for each horizon and sector
		
		forvalues i = 2/12 {
			gen r_`i'_gdp_dummy = (gdp_release_`i'_dummy + gdp_release_`=`i'-1'_dummy == 1)
		}
		
		
		* Rename
		
		keep vintages_date r_*_gdp_dummy
	
	save gdp_bench_r_dummies, replace
	
	
	
	/*----------------------
	Merge revisions with bench
	revisions datasets
	-----------------------*/
	
	use r_e_gdp_releases
	
		merge 1:1 vintages_date using gdp_bench_r_dummies
		
		drop _merge
		
	save gdp_bench_r_dummies_cleaned, replace
	
	
	
	
	/*----------------------
	r & e: Encompassing
	________________________
	Paper and presentation
	version
	-----------------------*/

	
	use gdp_bench_r_dummies_cleaned, clear
	
	
		* Keep common obs

		** Set common information using regression for model III (H1) to keep if !missing(residuals)

		qui {
			tsset vintages_date, monthly
			newey e_11_gdp c.r_11_gdp##i.r_11_gdp_dummy, lag(1) noconstant force
			predict residuals_aux, resid  // Generate the regression residuals.
		}

		keep if !missing(residuals_aux)  // Keep only the observations where the residuals are not missing.

		qui drop residuals_aux

	
		* Create a new frame named `encompassing_bench` to store regression results

		frame create encompassing_bench str32 variable int n str32 coef_1 str32 coef_2 str32 coef_3
		
		
		* Loop through variables e_`i'_gdp where `i' ranges from 2 to 12
		
		forval i = 2/11 {
			
			capture confirm variable e_`i'_gdp
			
			if !_rc {
				
				capture {
					
					tsset vintages_date
					
					quietly count if !missing(e_`i'_gdp)
					if r(N) < 5 continue  // Salta si hay menos de 5 observaciones
					
					newey e_`i'_gdp c.r_`i'_gdp##i.r_`i'_gdp_dummy, lag(1) noconstant force
					
					if _rc == 2001 {
						di in red "Insufficient observations for gdp_release_`i'"
						continue
					}
					
					matrix M = r(table)
					
					summarize e_`i'_gdp, detail
					local n = r(N)
					
					local coef_1 = M[1,1] // gdp_release_`i' 
					local coef_2 = M[1,3] // gdp_release_`i'_dummy
					local coef_3 = M[1,5] // gdp_release_`i'*gdp_release_`i'_dummy
					
					local pvalue_1 = M[4,1]
					local pvalue_2 = M[4,3]
					local pvalue_3 = M[4,5]
					
					local se_1 = M[2,1]
					local se_2 = M[2,3]
					local se_3 = M[2,5]
					
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
					
					if `pvalue_2' < 0.01 {
						local coef_2 = string(`coef_2', "%9.2f") + "***"
					}
					else if `pvalue_2' < 0.05 {
						local coef_2 = string(`coef_2', "%9.2f") + "**"
					}
					else if `pvalue_2' < 0.10 {
						local coef_2 = string(`coef_2', "%9.2f") + "*"
					}
					else {
						local coef_2 = string(`coef_2', "%9.2f")
					}
					
					if `pvalue_3' < 0.01 {
						local coef_3 = string(`coef_3', "%9.2f") + "***"
					}
					else if `pvalue_3' < 0.05 {
						local coef_3 = string(`coef_3', "%9.2f") + "**"
					}
					else if `pvalue_3' < 0.10 {
						local coef_3 = string(`coef_3', "%9.2f") + "*"
					}
					else {
						local coef_3 = string(`coef_3', "%9.2f")
					}
					
					
					*** Append standard error in parentheses to coef
					local coef_1 = "`coef_1'" + char(10) + "(" + string(`se_1', "%9.2f") + ")"
					local coef_2 = "`coef_2'" + char(10) + "(" + string(`se_2', "%9.2f") + ")"
					local coef_3 = "`coef_3'" + char(10) + "(" + string(`se_3', "%9.2f") + ")"

					
					frame post encompassing_bench ("e_`i'_gdp") (`n') ("`coef_1'") ("`coef_2'") ("`coef_3'")
				}
			}
			
			else {
				di in yellow "Variable e_`i'_gdp does not exist"
			}
		}

	frame change encompassing_bench

	list variable n coef_1 coef_2 coef_3, noobs clean
		
				
		* Rename vars
		
		rename variable h
		rename coef_1 Beta
		rename coef_2 Dummy
		rename coef_3 Interacción
		
		
		* Order vars
		
		order h n Beta Dummy Interacción
	
		
		* Export to excel file
		
		export excel using "$tables_folder/gdp_predictibility_noconstant_bench_newey.xlsx", ///
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
	
	