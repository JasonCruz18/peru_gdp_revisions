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
merged_df <- merged_df %>% filter(horizon < 11)


# Convert 'horizon' to a factor for categorical analysis
merged_df$horizon <- as.factor(merged_df$horizon)

# Further filter data for sector values within a specific range (-0.9 to 0.9)
df_filtered <- merged_df

# Display summary statistics of the filtered sector values
summary(df_filtered[[paste0("e_", sector)]])
summary(df_filtered[[paste0("z_", sector)]])



#........................
# e
#........................

# Ruta para guardar el archivo
output_file <- file.path(figures_dir, paste0("e_boxplot_", sector, "_m", ".png"))

# Abrir dispositivo gráfico PNG
png(filename = output_file, width = 10, height = 6, units = "in", res = 300)


# Create the boxplot without default axes
boxplot(
  e_gdp ~ horizon, 
  data = df_filtered, 
  outline = FALSE,
  xlab = "Horizonte",
  ylab = NA,
  col = "#FF0060",
  border = "#292929",
  lwd = 2.5,            # Grosor del contorno de las cajas
  cex.axis = 2.4,       # Tamaño de la fuente de los ejes
  cex.lab = 2.4,        # Tamaño de la fuente de las etiquetas de los ejes
  axes = FALSE          # Elimina los ejes predeterminados
)

# Redibujar los ejes sin marcas (ticks)
axis(1, lwd = 2.5, cex.axis = 2.4, tck = 0, at = 1:length(df_filtered$horizon), labels = df_filtered$horizon) # Eje X sin marcas
axis(2, lwd = 2.5, cex.axis = 2.4, tck = 0) # Eje Y sin marcas
box(lwd = 2.5) # Añade el contorno del gráfico

# Calculate group means
means <- tapply(df_filtered$e_gdp, df_filtered$horizon, mean, na.rm = TRUE)

# Add points for the means with a black border for the diamonds
points(
  x = 1:length(means), 
  y = means, 
  col = "#FF0060",       # Color del borde de los puntos
  pch = 21,            # Forma de los puntos
  cex = 2,           # Tamaño de los puntos
  bg = "#292929",         # Color de relleno de los puntos
  lwd = 2.5
)

# Add a legend for the mean
legend('top', horiz=TRUE, cex=1,
       c('Mean'),
       pch = 21,           # Tamaño de los puntos
       lwd = 2.5,
       inset=c(-0.2,0), bty='o', bg=rgb(1,1,1,.55), xpd=NA)


dev.off()

#........................
# z
#........................

# Ruta para guardar el archivo
output_file <- file.path(figures_dir, paste0("z_boxplot_", sector, "_m", ".png"))

# Abrir dispositivo gráfico PNG
png(filename = output_file, width = 10, height = 6, units = "in", res = 300)

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
  lwd = 2.5,            # Grosor del contorno de las cajas
  outpch = 19        # Forma para los outliers
  #outcol = "#292929"  # Color de los outliers
)

# Calculate group means
means <- tapply(df_filtered$e_gdp, df_filtered$horizon, mean, na.rm = TRUE)

# Add points for the means with a black border for the diamonds
points(
  x = 1:length(means), 
  y = means, 
  col = "#F6FA70",       # Color for the border of the diamonds
  pch = 16,            # Diamond shape
  cex = 1.5,           # Size of the diamonds
  bg = "white"         # Fill color (white) for the diamonds
)

# Add a legend for the mean
legend(
  "bottomleft", 
  legend = "Media", 
  pch = 16,            # Forma de los puntos
  cex = 1.5,           # Tamaño de los puntos
  bg = "white",         # Color de relleno de los puntos
  bty = "n",            # Sin cuadro alrededor de la leyenda
)


#dev.off()












library(ggplot2)

# Ruta para guardar el archivo
output_file <- file.path(figures_dir, paste0("e_boxplot_", sector, "_m", ".png"))

# Calcular los límites del eje y excluyendo los outliers
non_outlier_limits <- quantile(df_filtered$e_gdp, probs = c(0.05, 0.95), na.rm = TRUE)

# Crear el gráfico y guardarlo como PNG
ggplot(df_filtered, aes(x = as.factor(horizon), y = e_gdp)) +
  geom_boxplot(
    fill = "#FF0060", 
    color = "#292929", 
    size = 1.25,  # Grosor del contorno de las cajas
    outlier.shape = NA,  # Eliminar outliers del gráfico
    whisker.linetype = "dashed"  # Línea entrecortada para los bigotes
  ) +
  stat_summary(
    fun = mean, 
    geom = "point", 
    shape = 21, 
    size = 4, 
    color = "#FF0060",  # Color del borde de los puntos
    fill = "#292929",   # Color de relleno de los puntos
    stroke = 1.25        # Grosor del borde de los puntos
  ) +
  scale_y_continuous(limits = c(non_outlier_limits[1] - 0.1 * diff(non_outlier_limits), 
                                non_outlier_limits[2] + 0.1 * diff(non_outlier_limits))) +  # Escalar el eje y ajustando mejor las cajas
  labs(
    x = "Horizonte",
    y = NULL
  ) +
  theme_minimal(base_size = 14) +
  theme(
    axis.title.y = element_blank(),
    panel.grid.major = element_line(color = "#E5E5E5"),
    panel.grid.minor = element_blank(),
    axis.text = element_text(color = "black"),
    axis.title = element_text(color = "black"),
    plot.title = element_text(hjust = 0.5)
  ) +
  ggtitle("Distribución de e_gdp por horizonte") +
  geom_segment(
    aes(x = as.numeric(as.factor(horizon)), xend = as.numeric(as.factor(horizon)), 
        y = non_outlier_limits[1], yend = min(df_filtered$e_gdp, na.rm = TRUE)),
    color = "#292929", linetype = "dashed", size = 0.75
  ) +
  geom_segment(
    aes(x = as.numeric(as.factor(horizon)), xend = as.numeric(as.factor(horizon)), 
        y = non_outlier_limits[2], yend = max(df_filtered$e_gdp, na.rm = TRUE)),
    color = "#292929", linetype = "dashed", size = 0.75
  ) +
  geom_segment(
    aes(x = as.numeric(as.factor(horizon)) - 0.15, xend = as.numeric(as.factor(horizon)) + 0.15, 
        y = non_outlier_limits[1], yend = non_outlier_limits[1]),
    color = "#292929", size = 0.75
  ) +
  geom_segment(
    aes(x = as.numeric(as.factor(horizon)) - 0.15, xend = as.numeric(as.factor(horizon)) + 0.15, 
        y = non_outlier_limits[2], yend = non_outlier_limits[2]),
    color = "#292929", size = 0.75
  )

#ggsave(output_file, width = 10, height = 6, dpi = 300)






