/********************
Master Do-file
***

	Author
	---------------------
	Jason (for any issues email to jj.cruza@up.edu.pe)
	*********************/

	*** Program: master_do_file.do
	** 	First Created: 09/09/25
	** 	Last Updated: 09/09/25	

*** 
** This do-file runs all other do-files sequentially.
***



	/*----------------------
	Initial Setting
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

	*log using jefas_encompassing.txt, text replace // Opens a log file and replaces it if it exists.

	
	
	/*----------------------
	Defining workspace path
	------------------------*/

	di `"Please, enter your path for storing the (in/out)puts of this do-file in the COMMAND WINDOW and press ENTER."'  _request(path)
	
	cd "$path"

	
	
	/*----------------------
	Setting folders to store (in/out)puts
	------------------------*/

	shell mkdir "raw_data"		// Creating raw data folder.
	shell mkdir "input_data"	// Creating input data folder.
	shell mkdir "output" 		// Creating output folder.
*	shell mkdir "output/graphs" // Creating output charts folder.
	shell mkdir "output/tables" // Creating output tables folder.
			
		
	* Set as global vars
	
	global raw_data "raw_data"				// Use to raw data.
	global input_data "input_data"			// Use to import data.
*	global output_graphs "output/graphs"	// Use to export charts.
	global output_tables "output/tables"	// Use to export tables.
	
	

	/*----------------------
	Running Data Cleansing
	------------------------*/

	di "Running Data Cleansing..."
	do 1_data_cleansing.do		// Executes the first do-file for data cleaning.

	/*----------------------
	Running Errors Analysis
	------------------------*/

	di "Running Errors Analysis..."
	do 2_errors.do			// Executes the second do-file for errors analysis.

	/*----------------------
	Running Revisions Analysis
	------------------------*/

	di "Running Revisions Analysis..."
	do 3_revisions.do		// Executes the third do-file for revisions analysis.

	/*----------------------
	Running Nowcasting Analysis
	------------------------*/

	di "Running Nowcasting Analysis..."
	do 4_nowcasting.do		// Executes the fourth do-file for nowcasting ex-post exercise.

	di "All do-files executed successfully!"  // Final message confirming completion.

