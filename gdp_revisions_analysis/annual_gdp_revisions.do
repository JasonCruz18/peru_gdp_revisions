/********************
Analysis for Annual GDP Revisions
***

	Author
	---------------------
	Jason Cruz
	*********************/

	*** Program: annual_gdp_revisions
	** 	First Created: 27/10/23
	** 	Last Updated:  28/10/23
			
				
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

	di `"Please, input the path for storing the outputs of this dofile into the COMMAND WINDOW and then press ENTER  "'  _request(path)
	cd "$path"


/*----------------------
Import ODBC dataset and
save temp
-----------------------*/


	//import excel "documents_law", firstrow // In case you use your local path, make sure you have the "documents_law" file
					
	odbc load, exec("select * from annual_gdp_revisions") dsn("gdp_real_time_database") lowercase sqlshow clear // When we use ODBC server

	save TempData, replace
		
		
		
/*----------------------
Parsing dataset
-----------------------*/
		
		
use TempData, clear

	d // check entire dataset variables
	
	
	** Transform all variable names to lower case
	
	rename _all, lower
	
	
	** Vars label as vars names (lowercase)
	
	foreach var of varlist _all {
	label variable `var' "`var'"
	}
	

	** Checking null values and duplicates for a estimates
	
	/* duplicates list id_nota // There are duplicates
	
	sort documents
	quietly by documents:  gen dup = cond(_N==1,0,_n)
	tab dup 	
	
	duplicates drop documents, force
	
	duplicates list documents // (0 observations are duplicates)

	misstable summarize // checking null values (variables nonmissing or string)
	
	drop dup
	*/
	
	** Drop stop var and generate index var
	
	/* drop a
	gen index = _n
	*/
	
	** Checking row number
	
	count // 336 parsed observations

save "annual_gdp_revisions", replace


		
/*----------------------
Ploting for monthly GDP
revisions
-----------------------*/


use "annual_gdp_revisions", clear


	
	** Changing string var to date var

	
	// Crea una variable numérica a partir de la variable de año
	gen year_numeric = real(vintage_year)

	// Crea un gráfico de series de tiempo
	tsset year_numeric

	//gen month = substr(vintage_year, 1, 3)
	//gen year = substr(vintage_year, 5, 2)

//gen complete_year = cond(year < "50", "20" + year, "19" + year)
//gen date_str = "01" + month + complete_year

//gen fulldate = date("01" + "" + month + "" + complete_year, "DMY")
//format fulldate %td



	** First graph
	** -----------		

	
	** Set up the color palette
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	twoway (line revision year_numeric, lcolor("`r(p1)'%100") fintensity(*0.8)), ///
	xtitle("", axis(1)) ///
	ytitle("Revisiones del crecimiento del PBI") ///
	title("Revisiones del crecimiento del PBI (anual)", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(white)
	
	
	graph export "annual_gdp_revisions.pdf", as(pdf) replace
	graph export "annual_gdp_revisions.eps", as(eps) replace
	graph export "annual_gdp_revisions.png", as(png) replace
	
	
	** Second graph
	** -----------		

	
	rename mean mean_revisions
	
	grstyle init
	
	grstyle set plain, horizontal grid dotted
	
	** Set up the color palette
	
	
	colorpalette ///
	"25 57 65" ///
	"0 180 140" ///
	, n(2) nograph

	twoway (line revision year_numeric, lcolor("`r(p1)'%100") fintensity(*0.8)) ///
	(line mean_revisions year_numeric, bcolor("`r(p2)'")), ///
	xtitle("", axis(1)) ///
	legend(title("(%)", size(*0.55) position(12)) cols(1) label(1 "Revisiones (estimado definitivo menos primer estimado o preliminar)") label(2 "Promedio de las revisiones") size(*0.5) ring(0) position(3) bmargin(zero) color(gs1) order(1 2) region(col(white)) /*placement(s)*/ rowgap(0) colgap(*0.8)) ///
	ytitle("Revisiones del crecimiento del PBI") ///
	title("Revisiones del crecimiento del PBI (anual)", size(*0.55) box bexpand bcolor("`r(p1)'") color(white)) ///
	graphregion(color(white)) ///
	bgcolor(blue)
	
	
	graph export "annual_gdp_revisions_mean.pdf", as(pdf) replace
	graph export "annual_gdp_revisions_mean.eps", as(eps) replace
	graph export "annual_gdp_revisions_mean.png", as(png) replace
	
	