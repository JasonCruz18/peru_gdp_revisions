/********************
Efficiency Tests
***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: efficiency_tests.do
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
	//shell mkdir "output/graphs" 	// Creates folder to save graphs.
	shell mkdir "output/tables" 	// Creates folder to save tables.
	//shell mkdir "output/data" 		// Creates folder to save data.
		
	
	* Set as global vars
	
	//global graphs_folder "output/graphs"	// Use to export graphs.
	global tables_folder "output/tables"	// Use to export tables.
	//global data_folder "output/data"		// Use to export .dta.
	
		
		
	/*----------------------
	Import ODBC merged dataset and
	save temp
	-----------------------*/
											
	odbc load, exec("select * from sectorial_gdp_monthly_revisions_panel") dsn("gdp_revisions_datasets") lowercase sqlshow clear // Loads data from PostgresSQL using ODBC.
		
	
	save temp_data, replace


	
	/*----------------------
	Cleaning dataset
	-----------------------*/

	use temp_data.dta, clear

		* Order and sort
		
		order country year
		sort country year
		
		
		* At a glance
		
		d // Check entire dataset variables.
		sum // Summarize stats for entire dataset variables.
		count // How many obs?: 8,624.
		
		
		
		
		
		
		
		
		
		
		
		
	/*----------------------
	Drop aux data .dta
	-----------------------*/	

	// List all .dta files in the current directory and store in a local macro
	local dta_files : dir . files "*.dta"

	// Iterate over each .dta file and delete it
	foreach file of local dta_files {
		erase "`file'"
	}	
		
		
		
		
		