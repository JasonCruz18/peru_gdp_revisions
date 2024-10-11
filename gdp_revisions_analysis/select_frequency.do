* Ask the user to input the frequency
display "Select the frequency for the dataset:"
display "1. Annual"
display "2. Quarterly"
display "3. Monthly"

* Capture user input
input freq

* Define the frequency based on user input
local frequency = ""
if `freq' == 1 {
    local frequency "annual"
}
else if `freq' == 2 {
    local frequency "quarterly"
}
else if `freq' == 3 {
    local frequency "monthly"
}
else {
    display "Invalid selection. Please select 1, 2, or 3 and rerun the do-file."
    exit
}

* Display the selected frequency
display "You have selected the frequency: `frequency'"

* Save the frequency as a global macro
global frequency `frequency'

