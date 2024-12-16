#*******************************************************************************
# Boxplots: Forecast Errors by Horizon by Pooling Fixed-Event Forecasts
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: boxplot_m.R
# + First Created: 11/10/24
# + Last Updated: 12/15/24
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
  # Check if rstudioapi is available for folder selection
  user_path <- rstudioapi::selectDirectory() # Open directory selection dialog
  if (!is.null(user_path)) {
    setwd(user_path)  # Set the working directory to the selected folder
    cat("The working directory has been set to:", getwd(), "\n")
  } else {
    cat("No folder was selected.\n")  # If no folder was selected
  }
} else {
  cat("Install the 'rstudioapi' package to use this functionality.\n")  # If rstudioapi is not installed
}

# Define output directories
output_dir <- file.path(user_path, "charts")  # Main output directory

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



#*******************************************************************************
# Database Connection
#*******************************************************************************

# Retrieve database credentials from environment variables
user <- Sys.getenv("CIUP_SQL_USER")  # PostgreSQL username
password <- Sys.getenv("CIUP_SQL_PASS")  # PostgreSQL password
host <- Sys.getenv("CIUP_SQL_HOST")  # Host of the PostgreSQL server
port <- 5432  # Default PostgreSQL port
database <- "gdp_revisions_datasets"  # Database name

# Connect to PostgreSQL database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data from the first table
query_1 <- "SELECT * FROM e_sectorial_gdp_monthly_panel"
df_1 <- dbGetQuery(con, query_1)

# Fetch data from the second table
query_2 <- "SELECT * FROM z_sectorial_gdp_monthly_panel"
df_2 <- dbGetQuery(con, query_2)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Data Merging
#*******************************************************************************

# Merge the two datasets loaded from PostgreSQL using a full join
merged_df <- df_1 %>%
  full_join(df_2, by = c("vintages_date", "horizon"))  # Replace with actual common column names

# Sort merged_df by "vintages_date" and "horizon"
merged_df <- merged_df %>%
  arrange(vintages_date, horizon)

cat("Datasets merged successfully. Rows in merged data frame:", nrow(merged_df), "\n")



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Define the sectors to iterate over for plotting
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
             "construction", "commerce", "electricity", "services")

# Filter data to remove rows with missing values in key columns
merged_df <- merged_df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date))

# Filter data by horizon values (>1 & <11) for relevant analysis
merged_df <- merged_df %>% filter(horizon > 1 & horizon < 11)

# Convert 'horizon' to a factor for categorical analysis in the plots
merged_df$horizon <- as.factor(merged_df$horizon)



#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Function to create boxplots for e and z variables
generate_boxplot <- function(data, variable, color, legend_position, sector, output_dir) {
  # Path to save the plot
  output_file <- file.path(output_dir, paste0(variable, "_boxplot_", sector, "_m", ".png"))
  
  # Open PNG device to save the plot with specified size and resolution
  png(filename = output_file, width = 10, height = 6, units = "in", res = 300)
  
  # Set the background to transparent
  par(bg = "transparent", mar = c(2.0, 2.55, 1.2, 0.2)) # mar = c(bottom, left, top, right)
  
  # Create the boxplot with custom y-axis labels always showing one decimal
  boxplot(
    formula = as.formula(paste0(variable, "_", sector, " ~ horizon")), 
    data = data, 
    outline = FALSE,
    xlab = NA,
    ylab = NA,
    col = color,
    border = "#292929",  # Color for the border of the boxes
    lwd = 3.0,           # Box contour thickness
    cex.axis = 2.2,      # Axis font size
    cex.lab = 2.2,       # Label font size
    xaxt = "n",          # Suppress default x-axis to add custom ticks
    yaxt = "n"           # Suppress default y-axis to add custom ticks
  )
  
  # Custom x-axis labels (t+2, t+3, ..., t+10)
  axis(1, at = 1:length(levels(data$horizon)), labels = paste0("t+", 2:10), cex.axis = 2.2)
  
  # Add y-axis with default ticks and formatted labels (1 decimal place)
  y_ticks <- axTicks(2)  # Get default tick positions for y-axis
  axis(2, at = y_ticks, labels = sprintf("%.1f", y_ticks), cex.axis = 2.2, las=0)
  
  # Add a box around the plot
  box(lwd = 2.2)
  
  # Calculate group means for each horizon
  means <- tapply(data[[paste0(variable, "_", sector)]], data$horizon, mean, na.rm = TRUE)
  
  # Add points for the means with a black border for the diamonds
  points(
    x = 1:length(means), 
    y = means, 
    col = color,         # Border color of points
    pch = 21,            # Shape of points (diamonds)
    cex = 3.5,           # Point size
    bg = "black",        # Fill color with 50% transparency
    lwd = 2.0
  )
  
  # Add a legend for the mean
  legend(legend_position,
         legend = "Media", 
         col = color,
         pch = 21,            # Shape of points (diamonds)
         pt.cex = 3.5,        # Point size
         cex = 2.5,
         pt.bg = "black",     # Fill color with 50% transparency
         text.col = "black",  # Text color for the legend
         horiz = TRUE,        # Horizontal legend layout
         bty = "n",           # No box around the legend
         pt.lwd = 2.0         # Contour thickness of points in the legend
  )
  
  # Close PNG device to save the plot
  dev.off()
}



#*******************************************************************************
# Generate Plots for e and z for All Sectors
#*******************************************************************************

# Loop through each sector and generate boxplots for e and z variables
for (sector in sectors) {
  
  cat("Generating plot for sector:", sector, "\n")
  
  # Filter data for the current sector
  df_filtered <- merged_df %>% 
    filter(!is.na(.data[[paste0("e_", sector)]]) & !is.na(.data[[paste0("z_", sector)]]))
  
  # Generate plots for z (legend at bottomleft) and e (legend at bottomright)
  generate_boxplot(df_filtered, "z", "#0079FF", "bottomleft", sector, output_dir)  # Plot for z
  generate_boxplot(df_filtered, "e", "#FF0060", "bottomright", sector, output_dir)  # Plot for e
}

   