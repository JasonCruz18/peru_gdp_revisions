* Create a dialog box to select the frequency
dialog define select_frequency
    dialog setbutton "Annual" `=1'
    dialog setbutton "Quarterly" `=2'
    dialog setbutton "Monthly" `=3'

dialog show select_frequency, title("Select the frequency")

* Capture the selection
if r(button) == 1 {
    local frequency "annual"
}
else if r(button) == 2 {
    local frequency "quarterly"
}
else if r(button) == 3 {
    local frequency "monthly"
}
else {
    display "Invalid selection. Please run the do-file again."
    exit
}

* Display the selection to the user
display "You have selected the frequency: `frequency'"

* Save the frequency in a global macro
global frequency `frequency'
