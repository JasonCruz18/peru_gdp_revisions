/********************
Data Visualization for Monthly GDP Revisions by sector
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: sector_monthly_revisions.do
	** 	First Created: 04/10/24
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
Import ODBC dataset and
save temp
-----------------------*/

					
	odbc load, exec("select * from sector_monthly_revision") dsn("gdp_revisions_datasets") lowercase sqlshow clear // When we use ODBC server

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
	
	order id revision_date

save temp_data, replace
	
	
	
/*----------------------
Ploting monthly GDP
revisions by sector
-----------------------*/
	
	
use temp_data, clear	
	
	
	** First graph
	** ------------		

	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	twoway (line gdp_revision revision_date, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("GDP revisions") ///
	title("Global GDP Monthly Revisions", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	graph export "gdp_revisions_m.pdf", as(pdf) replace
	graph export "gdp_revisions_m.eps", as(eps) replace
	graph export "gdp_revisions_m.png", as(png) replace
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	