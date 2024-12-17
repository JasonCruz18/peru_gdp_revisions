#*******************************************************************************
# Stacked Bars: Sectoral Contribution to PBI Global by Horizon
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: sectorial_stacked_bars_m.R
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
library(purrr)        # For reducing data
library(hrbrthemes)   # For additional theme settings
library(viridis)      # For color palettes
library(knitr)        # For creating tables


#*******************************************************************************
# Initial Setup
#*******************************************************************************

# Use a dialog box to select the folder
# The script checks if RStudio's API is available and prompts the user to select a directory.
if (requireNamespace("rstudioapi", quietly = TRUE)) {
  user_path <- rstudioapi::selectDirectory()  # Prompt user to select directory
  if (!is.null(user_path)) {
    setwd(user_path)  # Set working directory to selected folder
    cat("The working directory has been set to:", getwd(), "\n")
  } else {
    stop("No folder selected. Exiting script.")  # Exit if no folder is selected
  }
} else {
  stop("Install the 'rstudioapi' package to use this functionality.")  # Stop if rstudioapi is not available
}

# Define output directories
# Create a subdirectory for saving charts if it doesn't exist already.
output_dir <- file.path(user_path, "charts")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")


#*******************************************************************************
# Database Connection
#*******************************************************************************

# Retrieve database credentials from environment variables
# The credentials are retrieved from the environment to ensure security.
user <- Sys.getenv("CIUP_SQL_USER")  # Retrieve user from environment
password <- Sys.getenv("CIUP_SQL_PASS")  # Retrieve password from environment
host <- Sys.getenv("CIUP_SQL_HOST")  # Retrieve host from environment
port <- 5432  # Set default PostgreSQL port
database <- "gdp_revisions_datasets"  # Define the database name

# Connect to PostgreSQL database
# Establish a connection to the database using the RPostgres package.
con <- dbConnect(RPostgres::Postgres(),
                 dbname = database,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Fetch data from the database
# Execute a SQL query to retrieve data from the specified table in the database.
query <- "SELECT * FROM r_sectorial_gdp_annual_panel"
df <- dbGetQuery(con, query)  # Store query result in df

# Close the database connection
# Disconnect from the database after the data is fetched.
dbDisconnect(con)


#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Define the sectors to iterate over for plotting
# The list of sectors will be used to generate sector-wise contributions to GDP.
sectors <- c("gdp", "agriculture", "mining",
             "electricity", "construction",
             "manufacturing", "commerce", "services")

# Ensure that the 'vintages_date' column is of type Date
# Convert the 'vintages_date' column to Date format if it is not already in that format.
df$vintages_date <- as.Date(df$vintages_date)

# Filter data to remove rows with missing values in key columns
# Exclude rows where 'horizon' or 'vintages_date' are NA, ensuring data integrity.
df <- df %>% 
  filter(!is.na(horizon) & !is.na(vintages_date))

# Filter data by horizon values (>1 & <11) for relevant analysis
# Filter the data to include only rows where the horizon is between 1 and 11.
df <- df %>% filter(horizon > 1 & horizon < 11)

# Convert 'horizon' to a factor for categorical analysis in the plots
# The 'horizon' column is converted to a factor to treat it as categorical data.
df$horizon <- as.factor(df$horizon)

# Drop specific sectors from the data (if required)
# Create a filtered dataframe based on the sectors for further analysis.
df_filtered <- df

# Create a list to store results if you want to save all calculations
# This list will hold intermediate results for each sector.
results <- list()

# Loop over each sector to compute mean values
# The loop computes the mean of each sector's contribution by horizon and stores the result.
for (sector in sectors) {
  # Use an explicit dataframe and dynamic column names
  temp_result <- df_filtered %>%
    group_by(horizon) %>%
    summarise(!!paste0("r_", sector, "_mean") := mean(.data[[paste0("r_", sector)]], na.rm = TRUE))
  
  # Save the result in the list
  results[[sector]] <- temp_result
}

# Combine the results into a single dataframe
# The list of results is merged into a single dataframe for further analysis.
final_result <- reduce(results, full_join, by = "horizon")


#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Transform the format of the data
# Reshape the final result dataframe into a long format suitable for plotting.
data_long <- gather(final_result, sector, value, r_gdp_mean:r_services_mean) %>%
  arrange(factor(horizon, levels = c("t+2", "t+3", "t+4", "t+5", "t+6", "t+7", "t+8", "t+9", "t+10"))) %>% 
  mutate(horizon=factor(horizon, levels=unique(horizon)))

# Display a preview of the reshaped data
# Output the first few rows of the reshaped data for inspection.
kable(head(data_long, 10))

# Separate data for r_gdp_mean and other sectors
# Filter the reshaped data into two groups: GDP-related and other sectors.
data_gdp <- data_long %>% filter(sector == "r_gdp_mean")
data_others <- data_long %>% filter(sector != "r_gdp_mean")

# Create the plot
# Generate the stacked bar chart with the specified formatting.
plot <- ggplot() + 
  # Stacked bars with outline and transparency
  geom_bar(data = data_others, 
           aes(fill = sector, y = value, x = horizon), 
           position = "stack", 
           stat = "identity", 
           color = "black",    # Black border for bars
           linewidth = 0.65,    # Set border thickness
           alpha = 0.85) +     # Set transparency to 85%
  
  # Line and points for r_gdp_mean
  geom_line(data = data_gdp, 
            aes(x = horizon, y = value, group = 1, 
                color = "r_gdp_mean", linetype = "r_gdp_mean"),  # Line label
            linewidth = 1.2) + 
  geom_point(data = data_gdp, 
             aes(x = horizon, y = value, 
                 color = "r_gdp_mean", shape = "r_gdp_mean"),     # Point label
             size = 4.5,          # Point size
             fill = "black",      # Point fill color
             stroke = 0.8) +        # Point border thickness
  
  # Customize the legend
  scale_color_manual(name = NULL, values = c("r_gdp_mean" = "#292929"), 
                     labels = c("PBI Global")) +  # Line color for GDP
  scale_linetype_manual(name = NULL, values = c("r_gdp_mean" = "solid"), 
                        labels = c("PBI Global")) + # Line style for GDP
  scale_shape_manual(name = NULL, values = c("r_gdp_mean" = 21), 
                     labels = c("PBI Global")) + # Point style for GDP
  
  # Set manual labels for bar legend
  scale_fill_manual(values = c("r_agriculture_mean" = "#FF0060", 
                               "r_mining_mean" = "#00DFA2", 
                               "r_manufacturing_mean" = "#0079FF", 
                               "r_electricity_mean" = "#292929", 
                               "r_construction_mean" = "#626E83", 
                               "r_commerce_mean" = "#F5F5F5", 
                               "r_services_mean" = "#F6FA70"), 
                    labels = c("Agropecuario", 
                               "Comercio", 
                               "Construcción", 
                               "Electricidad y Agua", 
                               "Manufactura", 
                               "Minería e Hidrocarburos", 
                               "Otros Servicios")) +
  
  # Customize plot appearance
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
    legend.text = element_text(size = 13, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.8),
    axis.line = element_line(color = "black", linewidth = 0.8),
    panel.border = element_rect(color = "black", linewidth = 0.8, fill = NA),
    plot.margin = margin(9, 5, 9, 4) # margin(top, right, bottom, left)
  ) +
  scale_x_discrete(
    breaks = 2:10,
    labels = paste0("t+", 2:10)
  ) +
  scale_y_continuous(labels = number_format(accuracy = 0.1)) +
  coord_cartesian(clip = "off")

# Print the plot
print(plot)

# Save the plot
ggsave(file.path(output_dir, paste0("sectorial_stacked_bars.png")), plot, width = 10, height = 6, dpi = 300)

