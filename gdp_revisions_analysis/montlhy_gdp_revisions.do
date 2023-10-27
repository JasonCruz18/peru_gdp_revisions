	/********************
	Analysis for Montlhy GDP Revisions
	***

		Author
		---------------------
		Jason Cruz
		*********************/

		*** Program: monthly_gdp_revisions
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
					
	odbc load, exec("select * from monthly_gdp_growth") dsn("gdp_real_time_database") lowercase sqlshow clear // When we use ODBC server

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
		
		count // 1,319 parsed observations

	save "monthly_gdp_revisions", replace

		
	/*----------------------
	Ploting for monthly GDP
	revisions
	-----------------------*/


	use "monthly_gdp_revisions", clear


	** Generate vars for first estimates and defintive estimates

	
	
