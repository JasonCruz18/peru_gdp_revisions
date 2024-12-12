#*******************************************************************************
# Boxplots: Backast Errors by Horizon by Pooling Fixed-Event Forecasts
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
library(tcltk)        # For creating GUI elements
library(scales)       # Format number



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
# Database Connection
#*******************************************************************************

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

# Merge the two datasets loaded form PostgresSQL
merged_df <- df1 %>%
  full_join(df2, by = c("vintages_date", "horizon")) # Replace with actual common column names

# Sort merged_df by “vintages_date” and “horizon”.
merged_df <- merged_df %>%
  arrange(vintages_date, horizon)

cat("Datasets merged successfully. Rows in merged data frame:", nrow(merged_df), "\n")



#*******************************************************************************
# Data Preparation
#*******************************************************************************

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
merged_df <- merged_df %>% 
  filter(!is.na(.data[[paste0("e_", sector)]]) & 
           !is.na(.data[[paste0("z_", sector)]]) & 
           !is.na(horizon) & 
           !is.na(vintages_date))

# Filter data by horizon values (< 13)
merged_df <- merged_df %>% filter(horizon < 13)


# Convert 'horizon' to a factor for categorical analysis
merged_df$horizon <- as.factor(merged_df$horizon)

# Further filter data for sector values within a specific range (-0.9 to 0.9)
df_filtered <- merged_df

# Display summary statistics of the filtered sector values
summary(df_filtered[[paste0("e_", sector)]])
summary(df_filtered[[paste0("z_", sector)]])



#*******************************************************************************
# Visualization
#*******************************************************************************

#........................
# e
#........................

# Create the boxplot
boxplot(
  e_gdp ~ horizon, 
  data = df_filtered, 
  outline = FALSE,
  main = "Boxplot of e_gdp by Horizon",
  xlab = "h",
  ylab = NA,
  col = "#FF0060",
  border = "#292929",
  lwd = 3,            # Grosor del contorno de las cajas
  outpch = 19,        # Forma para los outliers
  outcol = "#292929"  # Color de los outliers
)

# Calculate group means
means <- tapply(df_filtered$e_gdp, df_filtered$horizon, mean, na.rm = TRUE)

# Add points for the means with a black border for the diamonds
points(
  x = 1:length(means), 
  y = means, 
  col = "black",       # Color del borde de los puntos
  pch = 16,            # Forma de los puntos
  cex = 1.5,           # Tamaño de los puntos
  bg = "white"         # Color de relleno de los puntos
)

# Add a legend for the mean
legend(
  "top", 
  legend = "Media", 
  pch = 16,            # Forma de los puntos
  cex = 1.5,           # Tamaño de los puntos
  bg = "white",         # Color de relleno de los puntos
  bty = "n",            # Sin cuadro alrededor de la leyenda
)


# Save the plot as a PNG file
ggsave(file.path(figures_dir, paste0("e_boxplot_", sector, "_m", ".png")), 
       plot, width = 10, height = 6, dpi = 300)



#........................
# z
#........................


# Create the boxplot
boxplot(
  z_gdp ~ horizon, 
  data = df_filtered, 
  outline = FALSE,
  main = "Boxplot of e_gdp by Horizon",
  xlab = "h",
  ylab = NA,
  col = "#0079FF",
  border = "#292929",
  lwd = 3,            # Grosor del contorno de las cajas
  outpch = 19,        # Forma para los outliers
  outcol = "#292929"  # Color de los outliers
)

# Calculate group means
means <- tapply(df_filtered$e_gdp, df_filtered$horizon, mean, na.rm = TRUE)

# Add points for the means with a black border for the diamonds
points(
  x = 1:length(means), 
  y = means, 
  col = "black",       # Color for the border of the diamonds
  pch = 16,            # Diamond shape
  cex = 1.5,           # Size of the diamonds
  bg = "white"         # Fill color (white) for the diamonds
)

# Add a legend for the mean
legend(
  "top", 
  legend = "Media", 
  pch = 16,            # Forma de los puntos
  cex = 1.5,           # Tamaño de los puntos
  bg = "white",         # Color de relleno de los puntos
  bty = "n",            # Sin cuadro alrededor de la leyenda
)


# Save the plot as a PNG file
ggsave(file.path(figures_dir, paste0("z_boxplot_", sector, "_m", ".png")), 
       plot, width = 10, height = 6, dpi = 300)








