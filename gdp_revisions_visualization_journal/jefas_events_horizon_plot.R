#*******************************************************************************
# Lines: Specific Annual Events by Horizon (2006 & 2012)
#*******************************************************************************
#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: forecasts_events_by_horizon_a.R
# + First Created: 11/10/24
# + Last Updated: 09/13/25
#-------------------------------------------------------------------------------

#*******************************************************************************
# Libraries
#*******************************************************************************
library(RPostgres)
library(ggplot2)
library(lubridate)
library(svglite)
library(dplyr)
library(tidyr)
library(tcltk)
library(sandwich)
library(lmtest)
library(scales)
library(zoo)

#*******************************************************************************
# Initial Setup
#*******************************************************************************
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

output_dir <- file.path(user_path, "charts")
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
cat("Directories created successfully in:", user_path, "\n")

#*******************************************************************************
# Import from CSV file
#*******************************************************************************
file_path <- "C:/Users/Jason Cruz/OneDrive/Documentos/jefas_nowcasting_panel.csv"
df <- read.csv(file_path)

#*******************************************************************************
# Data Preparation
#*******************************************************************************
df <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

# New selected vintages: 2006m5 and 2012m7
selected_vintages <- as.Date(c("2006-05-01", "2012-07-01"))

df_filtered <- df %>%
  filter(target_period %in% selected_vintages) %>%
  mutate(
    vintage_label = case_when(
      target_period == as.Date("2006-05-01") ~ "2006m05",
      target_period == as.Date("2012-07-01") ~ "2012m07",
      TRUE ~ as.character(target_period)
    )
  )

# Wide → Long: add both release & release_hat
df_long <- df_filtered %>%
  select(horizon, vintage_label, gdp_release, gdp_release_hat) %>%
  pivot_longer(cols = c("gdp_release", "gdp_release_hat"),
               names_to = "series", values_to = "value") %>%
  mutate(
    series_label = case_when(
      series == "gdp_release" ~ vintage_label,
      series == "gdp_release_hat" ~ paste0(vintage_label, " now")
    ),
    type = ifelse(series == "gdp_release", "release", "nowcast")
  )



#*******************************************************************************
# Visualization (2006m05 and 2012m07)
#*******************************************************************************

# Limpieza preventiva (evita espacios/trailing mismatches)
df_long$series_label <- trimws(as.character(df_long$series_label))

# Definiciones (mantén los colores para nowcasts si los usas en la gráfica)
color_values <- c(
  "2006m05"     = "#3366FF",
  "2012m07"     = "#E6004C",
  "2006m05 now" = "#3366FF",
  "2012m07 now" = "#E6004C"
)

# Shapes para las RELEASES solamente
shape_values <- c(
  "2006m05" = 15,   # square
  "2012m07" = 17    # triangle
)

# Leyenda final (orden y etiquetas deseadas)
legend_breaks <- c("2006m05", "2012m07")
legend_labels <- c("May 2006", "July 2012")

horizon_plot_1 <- ggplot(df_long, aes(x = horizon, y = value, color = series_label, group = series_label)) +
  # Líneas (solid para release, dashed para nowcast)
  geom_line(aes(
    alpha    = ifelse(type == "nowcast", 0.75, 1),
    linetype = ifelse(type == "nowcast", "dashed", "solid")
  ), linewidth = 1.2) +
  
  # Puntos sólo para releases (shape mapeada por series_label)
  geom_point(
    data = subset(df_long, type == "release"),
    aes(shape = series_label),
    size = 6.0
  ) +
  
  scale_x_continuous(breaks = 1:12) +
  
  # Escala Y ajustada de 6.5 a 7.75 en saltos de 0.25
  scale_y_continuous(
    limits = c(6.5, 7.75),
    breaks = seq(6.5, 7.75, by = 0.25)
  ) +
  
  # IMPORTANTE: limitar los breaks a las 2 series que queremos en la leyenda
  scale_color_manual(
    values = color_values,
    breaks = legend_breaks,
    labels = legend_labels
  ) +
  scale_shape_manual(
    values = shape_values,
    breaks = legend_breaks,
    labels = legend_labels
  ) +
  
  scale_alpha_identity() +
  scale_linetype_identity() +
  
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  
  guides(
    # Solo mostrar la leyenda de color, pero con shapes personalizados en override.aes
    color = guide_legend(
      nrow = 1, byrow = TRUE,
      keywidth = 2, keyheight = 1.2,
      override.aes = list(
        shape    = unname(shape_values[legend_breaks]),  # garantizo orden correcto
        linetype = rep("solid", length(legend_breaks)),
        alpha    = rep(1, length(legend_breaks)),
        size     = rep(4, length(legend_breaks))
      )
    ),
    shape = "none"  # evitar duplicar la leyenda
  ) +
  
  theme_minimal() +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 20),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 20, color = "black"),
    plot.title = element_blank(),
    legend.position = c(0.985, 0.03),  # esquina inferior derecha dentro del panel
    legend.justification = c("right", "bottom"),
    legend.title = element_blank(),
    legend.text = element_text(size = 20, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 10, 9, 4)
  )

print(horizon_plot_1)

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.png"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       dpi = 300,       # Set DPI to 300 for high resolution
       bg = "white")

# Save as high-resolution PDF
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.pdf"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9)

# Save plot as EPS file
ggsave(filename = file.path(output_dir, "Fig_Nwc_1.eps"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       device = "eps",      # Set device to EPS
       dpi = 300,           # Ensure high resolution (300 DPI)
       bg = "white")        # White background




#*******************************************************************************
# Data Preparation
#*******************************************************************************
df <- df %>%
  mutate(
    target_period = gsub("m", "-", target_period),
    target_period = as.yearmon(target_period, "%Y-%m"),
    target_period = as.Date(target_period),
    horizon = as.numeric(horizon)
  )

# New selected vintages: 2006m5 and 2012m7
selected_vintages <- as.Date(c("2018-07-01", "2023-04-01"))

df_filtered <- df %>%
  filter(target_period %in% selected_vintages) %>%
  mutate(
    vintage_label = case_when(
      target_period == as.Date("2018-07-01") ~ "2018m07",
      target_period == as.Date("2023-04-01") ~ "2023m04",
      TRUE ~ as.character(target_period)
    )
  )

# Wide → Long: add both release & release_hat
df_long <- df_filtered %>%
  select(horizon, vintage_label, gdp_release, gdp_release_hat) %>%
  pivot_longer(cols = c("gdp_release", "gdp_release_hat"),
               names_to = "series", values_to = "value") %>%
  mutate(
    series_label = case_when(
      series == "gdp_release" ~ vintage_label,
      series == "gdp_release_hat" ~ paste0(vintage_label, " now")
    ),
    type = ifelse(series == "gdp_release", "release", "nowcast")
  )


#*******************************************************************************
# Visualization (2018m07 and 2023m04)
#*******************************************************************************


# Limpieza preventiva (evita espacios/trailing mismatches)
df_long$series_label <- trimws(as.character(df_long$series_label))

# Definiciones (mantén los colores para nowcasts si los usas en la gráfica)
color_values <- c(
  "2018m07"     = "#3366FF",
  "2023m04"     = "#E6004C",
  "2018m07 now" = "#3366FF",
  "2023m04 now" = "#E6004C"
)

# Shapes para las RELEASES solamente
shape_values <- c(
  "2018m07" = 15,   # square
  "2023m04" = 17    # triangle
)

# Leyenda final (orden y etiquetas deseadas)
legend_breaks <- c("2018m07", "2023m04")
legend_labels <- c("July 2018", "April 2023")

horizon_plot_2 <- ggplot(df_long, aes(x = horizon, y = value, color = series_label, group = series_label)) +
  # Líneas (solid para release, dashed para nowcast)
  geom_line(aes(
    alpha    = ifelse(type == "nowcast", 0.75, 1),
    linetype = ifelse(type == "nowcast", "dashed", "solid")
  ), linewidth = 1.2) +
  
  # Puntos sólo para releases (shape mapeada por series_label)
  geom_point(
    data = subset(df_long, type == "release"),
    aes(shape = series_label),
    size = 6.0
  ) +
  
  scale_x_continuous(breaks = 1:12) +
  
  # Escala Y ajustada de 6.5 a 7.75 en saltos de 0.25
  scale_y_continuous(
    limits = c(0.25, 2.75),
    breaks = seq(0.25, 2.75, by = 0.75)
  ) +
  
  # IMPORTANTE: limitar los breaks a las 2 series que queremos en la leyenda
  scale_color_manual(
    values = color_values,
    breaks = legend_breaks,
    labels = legend_labels
  ) +
  scale_shape_manual(
    values = shape_values,
    breaks = legend_breaks,
    labels = legend_labels
  ) +
  
  scale_alpha_identity() +
  scale_linetype_identity() +
  
  labs(x = NULL, y = NULL, title = NULL, color = NULL) +
  
  guides(
    # Solo mostrar la leyenda de color, pero con shapes personalizados en override.aes
    color = guide_legend(
      nrow = 1, byrow = TRUE,
      keywidth = 2, keyheight = 1.2,
      override.aes = list(
        shape    = unname(shape_values[legend_breaks]),  # garantizo orden correcto
        linetype = rep("solid", length(legend_breaks)),
        alpha    = rep(1, length(legend_breaks)),
        size     = rep(4, length(legend_breaks))
      )
    ),
    shape = "none"  # evitar duplicar la leyenda
  ) +
  
  theme_minimal() +
  theme(
    panel.grid.major = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.x = element_line(color = "#F5F5F5", linewidth = 0.8),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 20),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 20, color = "black"),
    plot.title = element_blank(),
    legend.position = c(0.985, 0.03),  # esquina inferior derecha dentro del panel
    legend.justification = c("right", "bottom"),
    legend.title = element_blank(),
    legend.text = element_text(size = 20, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 10, 9, 4)
  )

print(horizon_plot_2)

# Save high-resolution PNG (300 DPI)
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.png"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       dpi = 300,       # Set DPI to 300 for high resolution
       bg = "white")

# Save as high-resolution PDF
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.pdf"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9)

# Save plot as EPS file
ggsave(filename = file.path(output_dir, "Fig_Nwc_2.eps"), 
       plot = horizon_plot_1, 
       width = 16, 
       height = 9, 
       device = "eps",      # Set device to EPS
       dpi = 300,           # Ensure high resolution (300 DPI)
       bg = "white")        # White background
