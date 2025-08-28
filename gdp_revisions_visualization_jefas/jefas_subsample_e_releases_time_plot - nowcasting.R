#*******************************************************************************
# Lines and Bars: First, Most Recent Forecast and Final Revision   
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: e_releases_time_plot.R
# + First Created: 03/28/25
# + Last Updated: 03/29/25
#-------------------------------------------------------------------------------



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
library(scales)       # Format number
library(zoo)   # for as.yearmon

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
output_dir <- file.path(user_path, "charts")

# Create output directories if they do not exist
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

cat("Directories created successfully in:", user_path, "\n")



#*******************************************************************************
# Import from CSV file
#*******************************************************************************

# Define file path
file_path <- "C:/Users/Jason Cruz/OneDrive/Documentos/jefas_nowcasting_ts.csv"

# Read CSV into dataframe
df <- read.csv(file_path)

# If you prefer readr (tidyverse) for faster import:
# library(readr)
# df <- read_csv(file_path)



#*******************************************************************************
# Data Preparation
#*******************************************************************************

df <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period)
  )

# Define the sectors to iterate over for plotting
#sectors <- c("gdp", "agriculture", "fishing", "mining", "manufacturing", 
#             "electricity", "construction", "commerce", "services")
sectors <- c("gdp")

# target_period aAs Date

# df$target_period <- dmy_hms(df$target_period) # for imported csv

# Verificar el resultado
head(df$target_period)

# Ensure that the 'target_period' column is of type Date
df$target_period <- as.Date(df$target_period)


df <- df %>%
  mutate(
    gdp_most_recent = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_most_recent),
    gdp_release_1 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_1),
    gdp_release_2 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_2),
    gdp_release_3 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_3),
    gdp_release_hat_1 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_hat_1),
    gdp_release_hat_2 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_hat_2),
    gdp_release_hat_3 = ifelse(target_period >= as.Date("2020-03-01") & target_period <= as.Date("2021-10-01"), NaN, gdp_release_hat_3),
  )


#*******************************************************************************
# Suavizando
#*******************************************************************************

df <- df %>%
  mutate(
    gdp_most_recent_smooth = (lag(gdp_most_recent, 2) + 
                                2 * lag(gdp_most_recent, 1) + 
                                4 * gdp_most_recent + 
                                2 * lead(gdp_most_recent, 1) + 
                                lead(gdp_most_recent, 2)) / 10,
    
    gdp_release_1_smooth = (lag(gdp_release_1, 2) + 
                              2 * lag(gdp_release_1, 1) + 
                              4 * gdp_release_1 + 
                              2 * lead(gdp_release_1, 1) + 
                              lead(gdp_release_1, 2)) / 10,
    
    gdp_release_2_smooth = (lag(gdp_release_2, 2) + 
                              2 * lag(gdp_release_2, 1) + 
                              4 * gdp_release_2 + 
                              2 * lead(gdp_release_2, 1) + 
                              lead(gdp_release_2, 2)) / 10,
    
    gdp_release_3_smooth = (lag(gdp_release_3, 2) + 
                              2 * lag(gdp_release_3, 1) + 
                              4 * gdp_release_3 + 
                              2 * lead(gdp_release_3, 1) + 
                              lead(gdp_release_3, 2)) / 10,
    
    gdp_release_hat_1_smooth = (lag(gdp_release_hat_1, 2) + 
                              2 * lag(gdp_release_hat_1, 1) + 
                              4 * gdp_release_hat_1 + 
                              2 * lead(gdp_release_hat_1, 1) + 
                              lead(gdp_release_hat_1, 2)) / 10,
    
    gdp_release_hat_2_smooth = (lag(gdp_release_hat_2, 2) + 
                                  2 * lag(gdp_release_hat_2, 1) + 
                                  4 * gdp_release_hat_2 + 
                                  2 * lead(gdp_release_hat_2, 1) + 
                                  lead(gdp_release_hat_2, 2)) / 10,
    
    gdp_release_hat_3_smooth = (lag(gdp_release_hat_3, 2) + 
                                  2 * lag(gdp_release_hat_3, 1) + 
                                  4 * gdp_release_hat_3 + 
                                  2 * lead(gdp_release_hat_3, 1) + 
                                  lead(gdp_release_hat_3, 2)) / 10
  )


# Filter the dataframe for the desired date range
df_filtered <- df[df$target_period >= as.Date("2001-01-01") & df$target_period <= as.Date("2023-10-31"), ]



#*******************************************************************************
# Plotting Function
#*******************************************************************************

# Define the date label format function
date_label_format <- function(date) {
  paste0(format(date, "%Y"))
}


# Compute 1st nowcast error
df_filtered <- df_filtered %>%
  mutate(e_hat_1_gdp = gdp_most_recent_hat - gdp_release_hat_1)

# Create the graph
time_plot <- ggplot(df_filtered, aes(x = target_period)) +
  
  # Rectángulos de periodo de referencia con data explícita
  geom_rect(
    data = data.frame(
      xmin = as.Date("2013-01-01"),
      xmax = as.Date("2014-01-01"),
      ymin = -Inf,
      ymax = Inf,
      fill_label = "2007 base year"
    ),
    aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = fill_label),
    inherit.aes = FALSE,
    alpha = 0.70
  ) +
  geom_rect(
    data = data.frame(
      xmin = as.Date("2020-03-01"),
      xmax = as.Date("2021-10-01"),
      ymin = -Inf,
      ymax = Inf,
      fill_label = "COVID-19"
    ),
    aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax, fill = fill_label),
    inherit.aes = FALSE,
    alpha = 0.70
  ) +
  
  # Series
  geom_line(aes(y = gdp_most_recent_smooth, color = "Last release"), linewidth = 0.5) +
  geom_line(aes(y = gdp_release_1_smooth, color = "1st release"), linewidth = 0.85) +
  geom_line(aes(y = gdp_release_hat_1_smooth, color = "1st nowcast"), 
            linewidth = 0.70, alpha = 0.70) +   # transparency 50%
  
  # NEW: Forecast error bars (hat version)
  geom_bar(aes(y = e_hat_1_gdp * 2.0, fill = "1st nowcast error"), 
           stat = "identity", alpha = 0.45, color = "black", linewidth = 0.35) +
  
  # Reference line at 0
  geom_hline(yintercept = 0, color = "black", linewidth = 0.45) +
  
  # Last release points
  geom_point(aes(y = gdp_most_recent_smooth, color = "Last release"), size = 0.85, show.legend = FALSE) +
  
  labs(
    x = NULL,
    y = NULL,
    title = NULL,
    color = NULL,
    fill = NULL
  ) +
  theme_minimal() +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 16),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 16, color = "black"),
    plot.title = element_blank(),
    legend.position = "bottom",
    legend.title = element_blank(),
    legend.text = element_text(size = 16, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 5, 9, 4)
  ) +
  
  # Color scale
  scale_color_manual(values = c("1st release" = "#3366FF", 
                                "1st nowcast" = "#3366FF",  # same blue
                                "Last release" = "#E6004C")) +
  
  # Fill scale
  scale_fill_manual(values = c("1st nowcast error" = "#F5F5F5", 
                               "2007 base year" = "#00DFA2", 
                               "COVID-19" = "#FFF183")) +  
  
  # Adjusting legend order and forcing 2 rows for each box
  guides(
    color = guide_legend(order = 1, nrow = 2, byrow = TRUE),
    fill  = guide_legend(order = 2, nrow = 2, byrow = TRUE)
  ) +
  
  scale_y_continuous(
    breaks = scales::pretty_breaks(n = 5),
    labels = scales::number_format(accuracy = 0.1),
    sec.axis = sec_axis(~ . / 2.0, 
                        name = NULL, 
                        labels = scales::number_format(accuracy = 0.1))
  ) +
  scale_x_date(
    breaks = seq(as.Date("2001-01-01"), as.Date("2023-01-01"), by = "2 years"),
    date_labels = "%Y",
    limits = c(as.Date("2001-01-01"), as.Date("2023-01-01")),
    expand = c(0.02, 0.02)
  ) +
  coord_cartesian(ylim = c(-3.0, 11.3), clip = "off")  # Restrict Y-axis range

# Mostrar el gráfico
print(time_plot)



# Guardar el gráfico
plot_output_file <- file.path(output_dir, "e_nowcasts_releses_time_plot_by_subsample_1.png")
ggsave(filename = plot_output_file, plot = time_plot, width = 10, height = 6, dpi = 300, bg = "white")

