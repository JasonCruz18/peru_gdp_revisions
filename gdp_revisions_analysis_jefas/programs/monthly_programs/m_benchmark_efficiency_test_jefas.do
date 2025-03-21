/********************
Efficiency Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: m_efficiency_test.do
		** 	First Created: 09/15/24
		** 	Last Updated:  10/11/24
			
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
			
	//log using efficiency_tests.txt, text replace // Opens a log file and replaces it if it exists.

	

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
		
		
	odbc load, exec("select * from r_sectorial_gdp_monthly") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_ts, replace
	
	
	odbc load, exec("select * from r_sectorial_gdp_monthly_seasonal_dummies_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Change frequency to monthly, quarterly or annual to load dataset from SQL. 
		
	
	save r_dummies_panel, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(r)
	-----------------------*/
**# Bookmark #1

		
	use r_ts, clear
	
	
		* Format the date variable
		
		** Step 1: Divide by 1000 to convert from milliseconds to seconds
		
		gen vintages_seconds = vintages_date / 1000
	
		** Step 2: Convert seconds to a Stata date using the %tc format
		
		gen vintages_stata_date = vintages_seconds / 86400 // 86400 seconds in one day

		** Step 3: Format the new variable
		
		format vintages_stata_date %td
		
		** Step 4: Convert Stata date to monthly format
		
		gen vintages_monthly = mofd(vintages_stata_date)
		//gen vintages_quarterly = qofd(vintages_stata_date) // quarterly
		//gen vintages_annual = yofd(vintages_stata_date) // annual
		
		** Step 5: Monthly formatting
		
		format vintages_monthly %tm
		//format vintages_quarterly %tq // quarterly
		//format vintages_annual %ty // annual
		
		** Step 6: Drop 

		drop vintages_seconds vintages_stata_date vintages_date
		
		
		* Set vintages_monthly as first column
		
		order vintages_monthly
		
		
		* Sort by vintages_monthly
		
		sort vintages_monthly
		
		
		*** This is a provisional
		
		drop if vintages_monthly == tm(2000m4) | vintages_monthly == tm(2013m12)
		
		
		* Rename vintages_date
		
		rename vintages_monthly target_date

		
	save r_ts_cleaned, replace
	
	
	
	/*----------------------
	On-the-fly data cleaning
	(r-seasonal-dummies)
	-----------------------*/

	
	use r_dummies_panel, clear

	
		* Sort by vintages_date and horizon
		
		sort vintages_date horizon // Key step to set both the ID and time vars for panel data.

		
		* Fixing date format
		
		gen numeric_date = dofc(vintages_date) // To a Stata date in days.
		format numeric_date %td // To standard Stata date (e.g., day-month-year).

		gen target_date = mofd(numeric_date) // To a monthly date format.
		format target_date %tm // To standard Stata month (e.g., Jan 2023).

		drop vintages_date numeric_date // Drop the original vars since they are no longer needed.

		order target_date horizon // Reorder vars so that 'target_date' and 'horizon' appear first in the dataset.
		
		
		* Sort by target_date and horizon
		
		sort target_date horizon

		* Reshape from long to wide format
		
		reshape wide r_dummy_*, i(target_date) j(horizon)
		
		* Fix variable suffix

		global sectors gdp agriculture fishing mining manufacturing electricity construction commerce services

		foreach sector of global sectors {  
			foreach num of numlist 2/20 {  
				rename r_dummy_`sector'`num' r_dummy_`sector'_`num'  
			}  
		} 

	save r_dummies_ts_cleaned, replace

	
	
	/*----------------------
	Merge revisions and dummy
	time series datasets
	-----------------------*/
	

	use r_ts_cleaned, clear
	
		merge 1:1 target_date using r_dummies_ts_cleaned
	
	save r_dummies_ts_merged, replace
	
	

	/*----------------------
	r: regression on constant and
	bench dummy
	________________________
	Paper and presentation
	version
	-----------------------*/

	
	use r_dummies_ts_merged, clear

	frame create r_results str32 variable int n str32 coef_1 str32 coef_2 str8 sd str8 p1 str8 p99

	foreach sector in $sectors {
		
		forval i = 2/12 {
			
			capture confirm variable r_`i'_`sector'
			
			if !_rc {
				
				capture {
					
					tsset target_date
					
					quietly count if !missing(r_`i'_`sector')
					if r(N) < 5 continue  // Salta si hay menos de 5 observaciones
					
					newey r_`i'_`sector' r_dummy_`sector'_`i', lag(1) force
					
					if _rc == 2001 {
						di in red "Insufficient observations for r_`i'_`sector'"
						continue
					}
					
					matrix M = e(b)
					matrix P = e(p)
					
					summarize r_`i'_`sector', detail
					local n = r(N)
					local sd = string(r(sd), "%9.2f")
					local p1 = string(r(p1), "%9.2f")
					local p99 = string(r(p99), "%9.2f")
					
					local coef_1 = M[1,2]
					local coef_2 = M[1,1]
					local pvalue_1 = P[1,2]
					local pvalue_2 = P[1,1]
					
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
					
					frame post r_results ("r_`i'_`sector'") (`n') ("`coef_1'") ("`coef_2'") ("`sd'") ("`p1'") ("`p99'")
				}
			}
			
			else {
				di in yellow "Variable r_`i'_`sector' does not exist"
			}
		}
	}

	frame change r_results

	list variable n coef_1 coef_2 p1 p99 sd, noobs clean

	rename variable h
	rename coef_1 Insesgadez
	rename coef_2 Dummy
	rename p1 P1
	rename p99 P99
	rename sd SD

	order h n Insesgadez Dummy P1 P99 SD

	export excel using "$tables_folder/r_results.xlsx", firstrow(variable) replace
	
	
	
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
	
	