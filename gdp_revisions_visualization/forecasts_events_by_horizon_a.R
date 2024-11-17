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

# Define output directories
output_dir <- file.path(getwd(), "output")
figures_dir <- file.path(output_dir, "figures")
tables_dir <- file.path(output_dir, "tables")

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir)
if (!dir.exists(figures_dir)) dir.create(figures_dir)
if (!dir.exists(tables_dir)) dir.create(tables_dir)

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

# Select specific years and reshape data
df_filtered <- df_filtered %>% select(horizon, starts_with("year_199"))
df_long <- pivot_longer(df_filtered, cols = starts_with("year_"), 
                        names_to = "year", values_to = "value")

#*******************************************************************************
# Visualization
#*******************************************************************************

# Create the line plot
plot <- ggplot(df_long, aes(x = horizon, y = value, color = year, group = year)) +
  geom_line(linewidth = 1.3) +
  geom_point(size = 3) +
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  theme_minimal() +
  theme(
    panel.grid.major = element_line(linewidth = 1.3),
    panel.grid.minor = element_blank(),
    axis.text = element_text(color = "black", size = 14),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 12, color = "black"),
    legend.background = element_rect(color = "black", linewidth = 0.65),
    axis.line = element_line(color = "black", linewidth = 0.65),
    axis.ticks = element_line(color = "black", linewidth = 0.65),
    panel.border = element_rect(color = "black", linewidth = 0.65, fill = NA)
  ) +
  scale_color_manual(
    values = c("first_event" = "#FF0060", "second_event" = "#0079FF"),
    labels = c("first_event" = "1998", "second_event" = "1999")
  ) +
  coord_cartesian(clip = "off")

# Display the plot
print(plot)

# Save the plot as a PNG file
ggsave(file.path(figures_dir, paste0("motivation_", sector, ".png")), 
       plot, width = 10, height = 6, dpi = 300)


