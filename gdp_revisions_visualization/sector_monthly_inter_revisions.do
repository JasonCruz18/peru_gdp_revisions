/********************
Data Visualization for Monthly GDP Intermediate Revisions by sector
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: sector_monthly_inter_revisions.do
	** 	First Created: 04/16/24
	** 	Last Updated:  04/--/24
			
				
/*----------------------
Initial script configuration
-----------------------*/

	cls
	clear all
	set more off
	cap set maxvar 12000
	program drop _all
	

	
/*----------------------
Defining auxiliary path
------------------------*/

	di `"Please, enter your path for storing the outputs of this dofile in the COMMAND WINDOW and press ENTER  "'  _request(path)
	cd "$path"
	
	
/*----------------------
Setting folders to save outputs
------------------------*/
	
	shell mkdir output 		// to create folder to save outputs
	shell mkdir output/graphs // to create folder to save graphs
	
	* Define la ruta completa hacia la carpeta "graphs"
	
	global graphs_folder "output/graphs" // to export the graphics there
	
	
	
/*----------------------
Import ODBC dataset and
save temp
-----------------------*/

					
	odbc load, exec("select * from gdp_monthly_inter_revisions") dsn("gdp_revisions_datasets") lowercase sqlshow clear // When we use ODBC server

	save temp_data, replace
	
	
	
/*----------------------
Parsing and claning dataset
-----------------------*/
	

use temp_data, clear

	d // check entire dataset variables
		
		
	** Transform all variable names to lower case
	
	rename _all, lower
	
	
	** Vars label as vars names (lowercase)
	
	foreach var of varlist _all {
	label variable `var' "`var'"
	}
	
	
	** Check total observations 
	
	count // 148 observations
	
	** Order dataset
	
	order id inter_revision_date

save temp_data, replace



