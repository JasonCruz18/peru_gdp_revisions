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