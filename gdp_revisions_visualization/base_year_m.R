#*******************************************************************************
# Boxplots: Forecast Errors by Horizon by Pooling Fixed-Event Forecasts
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: base_year_m.R
# + First Created: 12/15/24
# + Last Updated: 12/15/24
#-------------------------------------------------------------------------------



#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages for various functionalities
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
output_dir <- file.path(user_path, "output")  # Main output directory
figures_dir <- file.path(output_dir, "figures")  # Directory for figures
tables_dir <- file.path(output_dir, "tables")  # Directory for tables

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
if (!dir.exists(figures_dir)) dir.create(figures_dir, recursive = TRUE)
if (!dir.exists(tables_dir)) dir.create(tables_dir, recursive = TRUE)

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
query_2 <- "SELECT * FROM e_sectorial_gdp_monthly_affected_panel"
df_2 <- dbGetQuery(con, query_2)

# Fetch data from the third table
query_3 <- "SELECT * FROM z_sectorial_gdp_monthly_panel"
df_3 <- dbGetQuery(con, query_3)

# Fetch data from the fourth table
query_4 <- "SELECT * FROM z_sectorial_gdp_monthly_affected_panel"
df_4 <- dbGetQuery(con, query_4)

# Close the database connection
dbDisconnect(con)



#*******************************************************************************
# Sector Selection
#*******************************************************************************

# Define available sectors for selection
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing",
             "electricity", "construction", "commerce", "services")

# GUI for sector selection using the Tcl/Tk library
selected_sector <- tclVar("gdp")  # Default sector set to 'gdp'
win <- tktoplevel()  # Create a new top-level window
tklabel(win, text = "Select a sector:") %>% tkpack()  # Label for selection
dropdown <- ttkcombobox(win, values = sectors, textvariable = selected_sector) %>% tkpack()  # Dropdown for sectors
tkbutton(win, text = "OK", command = function() tkdestroy(win)) %>% tkpack()  # OK button to close window

# Wait for user input and retrieve selected sector
tkwait.window(win)
sector <- tclvalue(selected_sector)



#*******************************************************************************
# Data Merging
#*******************************************************************************

# Merge all datasets using a full join sequentially on the "vintages_date" and "horizon" columns
merged_df <- df_1 %>%
  full_join(df_2, by = c("vintages_date", "horizon")) %>%
  full_join(df_3, by = c("vintages_date", "horizon")) %>%
  full_join(df_4, by = c("vintages_date", "horizon"))

# Sort the merged dataset by "vintages_date" and "horizon"
merged_df <- merged_df %>%
  arrange(vintages_date, horizon)

cat("All datasets merged successfully. Rows in merged data frame:", nrow(merged_df), "\n")



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Filter data to remove rows with missing values in key columns
merged_df <- merged_df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date))

# Filter data by horizon values (>1 & <11) for relevant analysis
merged_df <- merged_df %>% filter(horizon > 1 & horizon < 11)

# Convert 'horizon' to a factor for categorical analysis in the plots
merged_df$horizon <- as.factor(merged_df$horizon)



#*******************************************************************************
# Function to Generate Plots by Sector
#*******************************************************************************

# This function generates the plot for forecast errors by sector
generate_sector_plot <- function(sector, merged_df, figures_dir) {
  # Calculate the log of the average squared forecast errors by horizon for the 4 variables
  merged_df_mean <- merged_df %>%
    group_by(horizon) %>%
    summarise(
      e_sector_x_mean = log(mean(get(paste0("e_", sector, ".x"))^2, na.rm = TRUE)),
      e_sector_y_mean = log(mean(get(paste0("e_", sector, ".y"))^2, na.rm = TRUE)),
      z_sector_x_mean = log(mean(get(paste0("z_", sector, ".x"))^2, na.rm = TRUE)),
      z_sector_y_mean = log(mean(get(paste0("z_", sector, ".y"))^2, na.rm = TRUE))
    )
  
  # Plot the four averaged variables across the horizon
  plot <- ggplot(merged_df_mean, aes(x = horizon)) +
    # Lines and points for e-sector
    geom_line(aes(y = e_sector_x_mean, color = "e", group = 1), linewidth = 1.8) +
    geom_line(aes(y = e_sector_y_mean, color = "e (con año base)", group = 1), 
              linewidth = 1.8, linetype = "dashed", alpha = 0.65) +
    geom_point(aes(y = e_sector_x_mean, color = "e"), size = 4.2) +
    # Lines and points for z-sector
    geom_line(aes(y = z_sector_x_mean, color = "z", group = 1), linewidth = 1.8) +
    geom_line(aes(y = z_sector_y_mean, color = "z (con año base)", group = 1), 
              linewidth = 1.8, linetype = "dashed", alpha = 0.65) +
    geom_point(aes(y = z_sector_x_mean, color = "z"), size = 4.2) +
    # Theme and axis configuration
    labs(x = NULL, y = NULL, title = NULL, color = NULL) +
    theme_minimal() +
    theme(
      panel.grid.major = element_line(color = "#F5F5F5", linewidth = 1.2),
      panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 1.2),
      panel.grid.minor.y = element_blank(),
      axis.text = element_text(color = "black", size = 24),
      axis.ticks = element_line(color = "black"),
      axis.ticks.length = unit(0.1, "inches"),
      legend.position = "bottom",
      legend.title = element_blank(),
      legend.text = element_text(size = 24, color = "black"),
      legend.background = element_rect(fill = "white", color = "black", linewidth = 0.8),
      axis.line = element_line(color = "black", linewidth = 0.8),
      panel.border = element_rect(color = "black", linewidth = 0.8, fill = NA),
      plot.margin = margin(10, 10, 10, 10)
    ) +
    scale_color_manual(
      values = c(
        "e" = "#FF0060",
        "e (con año base)" = "#FF0060",
        "z" = "#0079FF",
        "z (con año base)" = "#0079FF"
      )
    ) +
    scale_x_discrete(
      breaks = 2:10,
      labels = paste0("t+", 2:10)
    ) +
    scale_y_continuous(labels = number_format(accuracy = 0.1)) +
    coord_cartesian(clip = "off")
  
  # Display and save the plot
  print(plot)
  
  ggsave(
    filename = file.path(figures_dir, paste0("base_year_", sector, ".png")),
    plot = plot, width = 10, height = 6, dpi = 300
  )
}

#*******************************************************************************
# Run the function
#*******************************************************************************

# Call the function with the selected sector
generate_sector_plot(sector = sector, merged_df = merged_df, figures_dir = figures_dir)

