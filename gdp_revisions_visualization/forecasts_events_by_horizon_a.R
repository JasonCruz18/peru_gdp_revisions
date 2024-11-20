#*******************************************************************************
# Lines: Specific Annual Events by Horizon
#*******************************************************************************

# Author: Jason Cruz
# Program: forecasts_events_by_horizon_a.do
# First Created: 09/20/24
# Last Updated: 11/16/24



#*******************************************************************************
# Libraries
#*******************************************************************************

# Load required packages for data processing and visualization
library(RPostgres)    # PostgreSQL database connection
library(ggplot2)      # Data visualization
library(lubridate)    # Date handling
library(svglite)      # SVG graphics export
library(dplyr)        # Data manipulation
library(tidyr)        # Data reshaping (pivot_longer)
library(tcltk)        # GUI elements for user input
library(sandwich)     # Robust standard errors
library(lmtest)       # Hypothesis testing



#*******************************************************************************
# Initial Setup
#*******************************************************************************

# Ask path directory to user where folders will be created
cat("Enter the path where you want to create the folders (leave empty to use current working directory): ")
user_path <- readline()

# If user doesn't provide a path, use the current working directory
if (user_path == "") {
  user_path <- getwd()
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
selected_sector <- tclVar("services")  # Default sector
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

# Fetch data for the selected sector
query <- sprintf("SELECT * FROM %s_annual_h_benchmark", sector)
df <- dbGetQuery(con, query)

# Close database connection
dbDisconnect(con)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

# Convert 'horizon' column to factor
df$horizon <- factor(df$horizon, levels = unique(df$horizon))

# Filter for specific horizons
df_filtered <- df %>% filter(horizon %in% c('t+1', 't+6', 't+12', 't+18', 't+24'))

# Select columns to plot (user-defined)
columns_to_plot <- c("year_1998", "year_1999")

# Transform the data: select relevant columns and reshape to long format
df_long <- df_filtered %>%
  select(horizon, all_of(columns_to_plot)) %>%  # Keep only the necessary columns
  pivot_longer(cols = all_of(columns_to_plot),  # Reshape to long format
               names_to = "year",              # Column for years
               values_to = "value") %>%        # Column for values
  mutate(year = gsub("year_", "", year))       # Remove 'year_' prefix from column names



#*******************************************************************************
# Visualization
#*******************************************************************************

# Create the dynamic plot
plot <- ggplot(df_long, aes(x = horizon, y = value, color = year, group = year)) +
  geom_line(linewidth = 1.5) +                 # Add lines for each series
  geom_point(size = 3.5) +                     # Add points for each series
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +  # Remove default labels
  theme_minimal() +                            # Apply minimal theme
  theme(
    panel.grid.major = element_line(linewidth = 1.3),  # Customize grid lines
    panel.grid.minor = element_blank(),               # Remove minor grid lines
    axis.text = element_text(color = "black", size = 24),  # Customize axis text
    legend.position = "bottom",                       # Place legend at the bottom
    legend.title = element_blank(),                   # Remove legend title
    legend.text = element_text(size = 24, color = "black"),  # Customize legend text
    legend.background = element_rect(color = "black", linewidth = 1.2),  # Add legend border
    axis.line = element_line(color = "black", linewidth = 0.8),  # Customize axis lines
    axis.ticks = element_line(color = "black", linewidth = 0.8),  # Customize axis ticks
    panel.border = element_rect(color = "black", linewidth = 0.8, fill = NA)  # Add panel border
  ) +
  scale_color_manual(
    values = setNames(c("#FF0060", "#0079FF"), c("1998", "1999")),  # Map colors to years
    labels = c("1998", "1999")  # Set legend labels
  ) +
  coord_cartesian(clip = "off")  # Prevent clipping of data outside panel

# Display the plot
print(plot)

# Save the plot as a PNG file
ggsave(file.path(figures_dir, paste0("motivation_", sector, ".png")), 
       plot, width = 10, height = 6, dpi = 300)


