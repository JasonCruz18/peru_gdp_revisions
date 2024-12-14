#*******************************************************************************
# Boxplots: Backcast Errors by Horizon by Pooling Fixed-Event Forecasts
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: e_boxplot.R
# + First Created: 11/10/24
# + Last Updated: 11/--/24
#-------------------------------------------------------------------------------

#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages
library(RPostgres)    # For connecting to PostgreSQL databases
library(ggplot2)      # For data visualization
library(lubridate)    # For date handling and manipulation
library(svglite)      # For creating SVG graphics
library(dplyr)        # For data manipulation and transformation
library(tidyr)        # For reshaping data
library(sandwich)     # For robust standard errors
library(lmtest)       # For hypothesis testing
library(scales)       # For formatting numbers
library(tcltk)        # For folder selection dialog

#*******************************************************************************
# Initial Setup
#*******************************************************************************

# Use a dialog box to select the folder
if (requireNamespace("rstudioapi", quietly = TRUE)) {
  user_path <- rstudioapi::selectDirectory()
  if (!is.null(user_path)) {
    setwd(user_path)
    cat("The working directory has been set to:", getwd(), "\n")
  } else {
    cat("No folder was selected.\n")
  }
} else {
  cat("Install the 'rstudioapi' package to use this functionality.\n")
}

# Define output directories
output_dir <- file.path(user_path, "output")
figures_dir <- file.path(output_dir, "figures")
tables_dir <- file.path(output_dir, "tables")

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
if (!dir.exists(figures_dir)) dir.create(figures_dir, recursive = TRUE)
if (!dir.exists(tables_dir)) dir.create(tables_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



#*******************************************************************************
# Sector Selection
#*******************************************************************************

# Define available sectors
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
             "construction", "commerce", "electricity", "services")

# GUI for sector selection
selected_sector <- tclVar("gdp")  # Default sector
win <- tktoplevel()
tklabel(win, text = "Select a sector:") %>% tkpack()
dropdown <- ttkcombobox(win, values = sectors, textvariable = selected_sector) %>% tkpack()
tkbutton(win, text = "OK", command = function() tkdestroy(win)) %>% tkpack()

# Wait for user input
tkwait.window(win)
sector <- tclvalue(selected_sector)



#*******************************************************************************
# Database Connection
#*******************************************************************************

# Retrieve database credentials from environment variables
user <- Sys.getenv("CIUP_SQL_USER")
password <- Sys.getenv("CIUP_SQL_PASS")
host <- Sys.getenv("CIUP_SQL_HOST")
port <- 5432
database <- "gdp_revisions_datasets"

# Connect to PostgreSQL database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data from the first table
query1 <- "SELECT * FROM e_sectorial_gdp_monthly_panel"
df1 <- dbGetQuery(con, query1)

# Fetch data from the second table
query2 <- "SELECT * FROM z_sectorial_gdp_monthly_panel"
df2 <- dbGetQuery(con, query2)

# Close the database connection
dbDisconnect(con)

#*******************************************************************************
# Data Merging
#*******************************************************************************

# Merge the two datasets loaded from PostgreSQL
merged_df <- df1 %>%
  full_join(df2, by = c("vintages_date", "horizon")) # Replace with actual common column names

# Sort merged_df by "vintages_date" and "horizon"
merged_df <- merged_df %>%
  arrange(vintages_date, horizon)

cat("Datasets merged successfully. Rows in merged data frame:", nrow(merged_df), "\n")

#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Define the sectors to iterate over
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
             "construction", "commerce", "electricity", "services")

# Filter data to remove rows with missing values in key columns
merged_df <- merged_df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date))

# Filter data by horizon values (< 13)
merged_df <- merged_df %>% filter(horizon < 11)

# Convert 'horizon' to a factor for categorical analysis
merged_df$horizon <- as.factor(merged_df$horizon)

#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Function to create boxplots for e and z variables
generate_boxplot <- function(data, variable, color, legend_position, sector, figures_dir) {
  # Path to save the plot
  output_file <- file.path(figures_dir, paste0(variable, "_boxplot_", sector, "_m", ".png"))
  
  # Open PNG device
  png(filename = output_file, width = 10, height = 6, units = "in", res = 300)
  
  # Create the boxplot without default axes
  boxplot(
    formula = as.formula(paste0(variable, "_", sector, " ~ horizon")), 
    data = data, 
    outline = FALSE,
    xlab = NA,
    ylab = NA,
    col = color,
    border = "#292929",
    lwd = 3.0,            # Box contour thickness
    cex.axis = 2.0,       # Axis font size
    cex.lab = 2.0         # Label font size
  )
  
  # Calculate group means
  means <- tapply(data[[paste0(variable, "_", sector)]], data$horizon, mean, na.rm = TRUE)
  
  # Add points for the means with a black border for the diamonds
  points(
    x = 1:length(means), 
    y = means, 
    col = color,         # Border color of points
    pch = 21,            # Shape of points
    cex = 2.0,           # Point size
    bg = "black",       # Fill color with 50% transparency
    lwd = 1.5
  )
  
  # Add a legend for the mean
  legend(legend_position,
         legend = "Mean", 
         col = color,
         pch = 21,            # Shape of points
         pt.cex = 2.0,        # Point size
         cex = 2.0,
         pt.bg = "black",    # Fill color with 50% transparency
         text.col = "black", # Text color
         horiz = TRUE,
         bty = "n",
         pt.lwd = 1.5         # Contour thickness of points
  )
  
  # Close PNG device
  dev.off()
}

#*******************************************************************************
# Generate Plots for e and z for All Sectors
#*******************************************************************************

for (sector in sectors) {
  # Filter data for the current sector
  df_filtered <- merged_df %>% 
    filter(!is.na(.data[[paste0("e_", sector)]]) & !is.na(.data[[paste0("z_", sector)]]))
  
  # Generate plots for z (legend at bottomleft) and e (legend at bottomright)
  generate_boxplot(df_filtered, "z", "#0079FF", "bottomleft", sector, figures_dir)  # Plot for z
  generate_boxplot(df_filtered, "e", "#FF0060", "bottomright", sector, figures_dir)  # Plot for e
}

