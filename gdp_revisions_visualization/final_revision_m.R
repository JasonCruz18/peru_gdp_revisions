#*******************************************************************************
# Lines and Bars: First, Most Recent Forecast and Final Revision   
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: final_revision_m.R
# + First Created: 12/16/24
# + Last Updated: 12/16/24
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
library(scales)       # For formatting numbers



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
    stop("No folder selected. Exiting script.")
  }
} else {
  stop("Install the 'rstudioapi' package to use this functionality.")
}

# Define output directories
output_dir <- file.path(user_path, "charts")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



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

# Fetch data
query <- "SELECT * FROM r_sectorial_gdp_annual_releases"
df <- dbGetQuery(con, query)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Define the sectors to iterate over for plotting
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
             "electricity", "construction", "commerce", "services")

# Ensure that the 'vintages_date' column is of type Date
df$vintages_date <- as.Date(df$vintages_date)

# Create new columns for forecast errors
for (sector in sectors) {
  df[[paste0("r_final_", sector)]] <- df[[paste0(sector, "_most_recent")]] - df[[paste0(sector, "_release_1")]]
}

# Filter the dataframe for the desired date range
df_filtered <- df[df$vintages_date >= as.Date("1993-01-01") & df$vintages_date <= as.Date("2025-01-01"), ]



#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Define the date label format function
date_label_format <- function(date) {
  paste0(format(date, "%Y"))
}

# Generate a sequence of dates for the first month of each year
breaks_dates <- seq(from = as.Date("1993-01-01"), 
                    to = as.Date("2025-01-01"), 
                    by = "3 year")

# Iterate over the sectors to create and save plots
for (sector in sectors) {
  
  cat("Generating plot for sector:", sector, "\n")
  
  # Calculate the range of the values for lines and bars
  range_lines <- max(c(df_filtered[[paste0(sector, "_release_1")]], df_filtered[[paste0(sector, "_most_recent")]]), na.rm = TRUE) -
    min(c(df_filtered[[paste0(sector, "_release_1")]], df_filtered[[paste0(sector, "_most_recent")]]), na.rm = TRUE)
  range_bars <- max(df_filtered[[paste0("r_final_", sector)]], na.rm = TRUE) - min(df_filtered[[paste0("r_final_", sector)]], na.rm = TRUE)
  
  # Calculate the scale factor for plotting
  scale_factor <- ifelse(range_bars == 0, 1, range_lines / range_bars)
  
  # Create the graph
  plot <- ggplot(df_filtered, aes(x = vintages_date)) +
    geom_line(aes(y = df_filtered[[paste0(sector, "_most_recent")]], color = "Predicción más reciente"), linewidth = 0.5) +
    geom_line(aes(y = df_filtered[[paste0(sector, "_release_1")]], color = "Predicción inicial"), linewidth = 1.25) +
    geom_bar(aes(y = df_filtered[[paste0("r_final_", sector)]] * scale_factor, fill = "Error de predicción inicial"), 
             stat = "identity", alpha = 0.85, color = "black", linewidth = 0.4)  +  # Add bars for Final Revision, scaled
    geom_hline(yintercept = 0, color = "black", linewidth = 1.2) +  # Add a horizontal line at y = 0
    geom_point(aes(y = df_filtered[[paste0(sector, "_most_recent")]], color = "Predicción más reciente"), size = 2.5) +  # Add points for Most Recent line
    labs(
      x = NULL,
      y = NULL,
      title = NULL,
      color = NULL,  # Remove the title from the color legend
      fill = NULL    # Remove the title from the fill legend
    ) +
    theme_minimal() +
    theme(
      panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),
      panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
      panel.grid.minor.y = element_blank(),
      axis.text = element_text(color = "black", size = 24), # optional size: 32
      axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),  # Center y-axis labels
      axis.ticks = element_line(color = "black"),  # Add ticks to both axes
      axis.ticks.length = unit(0.1, "inches"),  # Increase tick length
      axis.title.x = element_blank(),  # Remove the X-axis title
      axis.title.y = element_blank(),  # Remove the Y-axis title
      plot.title = element_blank(),  # Remove the plot title
      axis.title.y.right = element_blank(),  # Remove the title of the right Y-axis
      axis.text.x = element_text(angle = 0),  # Rotate X-axis labels
      legend.position = "bottom",
      legend.title = element_blank(),
      legend.text = element_text(size = 18, color = "black"),
      legend.background = element_rect(fill = "white", color = "black", linewidth = 0.8),
      axis.line = element_line(color = "black", linewidth = 0.8),
      panel.border = element_rect(color = "black", linewidth = 0.8, fill = NA),
      plot.margin = margin(9, 5, 9, 4) # margin(top, right, bottom, left)
    ) +
    scale_color_manual(values = c("Predicción inicial" = "#0079FF", "Predicción más reciente" = "#FF0060")) +  # Set colors for lines
    scale_fill_manual(values = c("Error de predicción inicial" = "#F5F5F5")) +  # Set color for bars
    scale_y_continuous(
      breaks = scales::pretty_breaks(n = 5),  # Adjust Y-axis breaks to fit the data
      labels = scales::number_format(accuracy = 0.1),  # Format primary Y-axis labels with 1 decimal place
      sec.axis = sec_axis(~ . / scale_factor, name = paste0(sector, " Error de predicción inicial"), labels = scales::number_format(accuracy = 0.1))  # Format secondary Y-axis labels with 1 decimal place
    ) +
    scale_x_date(
      breaks = breaks_dates,  # Set X-axis breaks for the first month of each year
      labels = date_label_format,  # Use custom date label format
      expand = c(0.02, 0.02)  # Remove extra margins on the left and right sides
    ) +
    coord_cartesian(clip = "off")  # Allow elements to be drawn outside the panel area
  
  # Display the plot
  print(plot)
  
  # Save the plot
  ggsave(file.path(output_dir, paste0(sector, "_releases_r_final.png")), plot, width = 10, height = 6, dpi = 300)
}

