#*******************************************************************************
# Box plots: backast errors by horizon by pooling fixed-event forecasts
#*******************************************************************************

# Author
# ---------------------
# Jason Cruz
# *********************
# *** Program: e_boxplot.do
# **  First Created: 11/16/24
# **  Last Updated:  11/--/24
#*******************************************************************************

#...............................................................................
# Libraries
#...............................................................................

# Load required packages
library(RPostgres)    # For connecting to PostgreSQL databases
library(ggplot2)      # For data visualization
library(lubridate)    # For date handling and manipulation
library(svglite)      # For creating SVG graphics
library(dplyr)        # For data manipulation and transformation
library(tidyr)        # For reshaping data
library(sandwich)     # For robust standard errors
library(lmtest)       # For hypothesis testing
library(tcltk)        # For creating GUI elements

#...............................................................................
# Initial Setup
#...............................................................................

# Define output directories
output_dir <- file.path(getwd(), "output")         # Base directory for output files
figures_dir <- file.path(output_dir, "figures")    # Directory for figures
tables_dir <- file.path(output_dir, "tables")      # Directory for tables

# Create directories if they do not exist
dir.create(output_dir, showWarnings = FALSE)
dir.create(figures_dir, showWarnings = FALSE)
dir.create(tables_dir, showWarnings = FALSE)

#...............................................................................
# Database Connection
#...............................................................................

# Retrieve environment variables for database credentials
user <- Sys.getenv("CIUP_SQL_USER")   # Database username
password <- Sys.getenv("CIUP_SQL_PASS")  # Database password
host <- Sys.getenv("CIUP_SQL_HOST")    # Database host
port <- 5432                           # Default PostgreSQL port
database <- "gdp_revisions_datasets"   # Name of the database

# Establish connection to the PostgreSQL database
con <- dbConnect(RPostgres::Postgres(), 
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data from the selected table
query <- "SELECT * FROM sectorial_gdp_monthly_cum_revisions_panel"
df <- dbGetQuery(con, query)

# Close the database connection
dbDisconnect(con)

#...............................................................................
# Data Preparation
#...............................................................................

# Prompt the user to select a sector via a GUI
sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
             "construction", "commerce", "electricity", "services")

selected_sector <- tclVar("gdp")  # Default sector value

# Create a selection window
win <- tktoplevel()
tklabel(win, text = "Select a sector:") %>% tkpack()
dropdown <- ttkcombobox(win, values = sectors, textvariable = selected_sector) %>% tkpack()
tkbutton(win, text = "OK", command = function() tkdestroy(win)) %>% tkpack()

# Wait for the user to make a selection
tkwait.window(win)

# Retrieve the selected sector
sector <- tclvalue(selected_sector)

# Filter data to remove rows with missing values in key columns
df <- df %>% 
  filter(!is.na(.data[[paste0("e_", sector)]]) & 
           !is.na(horizon) & 
           !is.na(vintages_date))

# Filter data by horizon values (< 20)
df <- df %>% filter(horizon < 20)

# Convert 'horizon' to a factor for categorical analysis
df$horizon <- as.factor(df$horizon)

# Further filter data for sector values within a specific range (-0.9 to 0.9)
df_filtered <- df %>%
  filter(.data[[paste0("e_", sector)]] >= -0.9 & .data[[paste0("e_", sector)]] <= 0.9)

# Display summary statistics of the filtered sector values
summary(df_filtered[[paste0("e_", sector)]])

#...............................................................................
# Visualization
#...............................................................................

# Create a boxplot for the filtered sector data
ggplot(df_filtered, aes(x = factor(horizon), y = .data[[paste0("e_", sector)]])) +
  geom_boxplot() +                                    # Add boxplot layer
  labs(
    title = "Boxplot of GDP Revisions",               # Set the plot title
    x = "Horizon",                                    # Label for x-axis
    y = "Revision Values"                             # Label for y-axis
  ) +
  theme_minimal()                                     # Apply a minimal theme





