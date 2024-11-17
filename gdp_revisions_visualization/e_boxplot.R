#*******************************************************************************
# Boxplots: Backast Errors by Horizon by Pooling Fixed-Event Forecasts
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
#df_filtered <- df

df_filtered <- df %>%
  filter(.data[[paste0("e_", sector)]] >= -2.5 & .data[[paste0("e_", sector)]] <= 5)

# Display summary statistics of the filtered sector values
summary(df_filtered[[paste0("e_", sector)]])

#...............................................................................
# Visualization
#...............................................................................

# Calcular la mediana para cada horizonte
median_data <- df_filtered %>%
  group_by(horizon = factor(horizon)) %>%
  summarize(
    median_value = mean(.data[[paste0("e_", sector)]], na.rm = TRUE),
    .groups = "drop"
  )

# Crear el boxplot minimalista
ggplot(df_filtered, aes(x = factor(horizon), y = .data[[paste0("e_", sector)]])) +
  geom_boxplot(
    color = "black",                                # Color de los bordes y bigotes
    fill = NA,                                        # Sin relleno en las cajas
    linewidth = 1.5,                                    # Grosor de los bordes y bigotes
    outlier.shape = 4,                                # Outliers como contornos
    outlier.color = "white",                        # Contornos de outliers
    outlier.size = 1,                                  # Grosor de los outliers
    outlier.stroke = 1.5
    ) +
  geom_segment(
    data = median_data,                               # Datos con medianas calculadas
    aes(
      x = as.numeric(horizon) - 0.33,                  # Ajuste horizontal para centrado
      xend = as.numeric(horizon) + 0.33,               # Extensión horizontal
      y = median_value, yend = median_value           # Coordenadas de la mediana
    ),
    color = "#0079FF", linewidth = 2.5                  # Color y grosor de la línea de la mediana
  ) +
  labs(
    #title = "Minimalist Boxplot of GDP Revisions",    # Título
    x = "Horizon",                                    # Etiqueta eje X
    y = "Revision Values"                             # Etiqueta eje Y
  ) +
  theme_minimal(base_size = 14) +                     # Tema minimalista
  theme(
    #plot.title = element_text(hjust = 0.5, face = "bold"), # Título centrado y en negrita
    axis.text = element_text(color = "black"),       # Color del texto de los ejes
    #axis.title = element_text(face = "bold"),         # Ejes en negrita
    panel.grid.major = element_blank(),              # Sin rejillas mayores
    panel.grid.minor = element_blank(),              # Sin rejillas menores
    panel.background = element_rect(fill = "white", color = NA) # Fondo blanco puro
  )






