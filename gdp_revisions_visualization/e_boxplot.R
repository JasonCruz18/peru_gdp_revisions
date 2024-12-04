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

# Fetch data from the selected table
query <- "SELECT * FROM z_sectorial_gdp_monthly_panel"
df <- dbGetQuery(con, query)

# Close the database connection
dbDisconnect(con)



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
df <- df %>% 
  filter(!is.na(.data[[paste0("e_", sector)]]) & 
           !is.na(horizon) & 
           !is.na(vintages_date))

# Filter data by horizon values (< 20)
df <- df %>% filter(horizon < 21)


# Convert 'horizon' to a factor for categorical analysis
df$horizon <- as.factor(df$horizon)

# Further filter data for sector values within a specific range (-0.9 to 0.9)
df_filtered <- df

#df_filtered <- df %>%
#  filter(.data[[paste0("e_", sector)]] >= -10 & .data[[paste0("e_", sector)]] <= 10)

# Display summary statistics of the filtered sector values
summary(df_filtered[[paste0("e_", sector)]])



#*******************************************************************************
# Visualization
#*******************************************************************************

# Calcular la media para cada horizonte
media_data <- df_filtered %>%
  group_by(horizon = factor(horizon)) %>%
  summarize(
    media_value = mean(.data[[paste0("e_", sector)]], na.rm = TRUE),
    .groups = "drop"
  )

# Calcular los cuartiles y los valores de los bigotes para el boxplot
df_filtered <- df_filtered %>%
  mutate(
    q1 = quantile(.data[[paste0("e_", sector)]], 0.25, na.rm = TRUE),  # Primer cuartil
    q3 = quantile(.data[[paste0("e_", sector)]], 0.75, na.rm = TRUE),  # Tercer cuartil
    iqr = q3 - q1,  # Rango intercuartílico
    lower_bound = q1 - 1.5 * iqr,  # Límite inferior de los bigotes
    upper_bound = q3 + 1.5 * iqr  # Límite superior de los bigotes
  )

# Filtrar los datos para eliminar los outliers
df_filtered_no_outliers <- df_filtered %>%
  filter(
    .data[[paste0("e_", sector)]] >= lower_bound & .data[[paste0("e_", sector)]] <= upper_bound
  )

# Calcular el ancho de la caja para ajustar la línea de la mediana
box_width <- 0.8  # Ancho de las cajas (ajustable según preferencias)

# Crear el boxplot con ajuste automático del ancho de la mediana
plot <- ggplot(df_filtered_no_outliers, aes(x = factor(horizon), y = .data[[paste0("e_", sector)]])) +
  geom_boxplot(
    width = box_width,                                  # Ancho de las cajas
    color = "#222831",                                  # Color de los bordes y bigotes
    fill = alpha("#F5F5F5", 0.65),                      # Relleno con transparencia
    linewidth = 1.2,                                    # Grosor de los bordes y bigotes
    outlier.shape = NA,                                  # Forma de los outliers
    outlier.color = NA,                          # Color para los outliers
    outlier.size = 1.2,                                 # Tamaño de los outliers
    outlier.stroke = 1.2                                # Grosor del borde de los outliers
  ) +
  geom_segment(
    data = media_data,
    aes(
      x = as.numeric(horizon) - box_width / 2,          # Ajuste dinámico basado en el ancho
      xend = as.numeric(horizon) + box_width / 2,       # Ajuste dinámico basado en el ancho
      y = media_value, 
      yend = media_value,
      color = "Media"                                   # Mapeo estático para incluir en la leyenda
    ),
    linewidth = 1.8
  ) +
  scale_color_manual(
    values = c("Media" = "#0079FF")                     # Define el color de la línea de la mediana
  ) +
  labs(
    x = NULL, y = NULL, title = NULL, color = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 1.2),
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 1.2),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 24),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 24, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.8),
    axis.line = element_line(color = "black", linewidth = 0.8),
    panel.border = element_rect(color = "black", linewidth = 0.8, fill = NA),
    plot.margin = margin(10, 10, 10, 10)                # Márgenes: top, right, bottom, left
  ) +
  scale_y_continuous(labels = number_format(accuracy = 0.1)) +
  coord_cartesian(clip = "off")

print(plot)

# Save the plot as a PNG file
#ggsave(file.path(figures_dir, paste0("e_boxplot_", sector, "_m_2", ".png")), 
#       plot, width = 10, height = 6, dpi = 300)
  



# Filter data by horizon values (< 20)
df <- df %>% filter(horizon < 13)


# Convert 'horizon' to a factor for categorical analysis
df$horizon <- as.factor(df$horizon)

value = c(df$z_gdp, df$z_agriculture)

boxplot(value + df$horizon, outline=FALSE)


# Crear un dataframe largo para combinar las dos variables
df_long <- data.frame(
  value = c(df$z_gdp, df$z_agriculture),
  variable = rep(c("z_gdp", "z_agriculture"), each = nrow(df)),
  horizon = rep(df$horizon, 2)
)

# Crear el boxplot
boxplot(value ~ variable + horizon, data = df_long, outline = FALSE, 
        xlab = "Variable y Horizonte", ylab = "Valores", col = c("lightblue", "lightgreen"),
        las = 2)




