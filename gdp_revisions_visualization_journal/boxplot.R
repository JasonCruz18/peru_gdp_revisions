#*******************************************************************************
# Boxplots: Forecast Errors by Horizon by Pooling Fixed-Event Forecasts
#*******************************************************************************

#-------------------------------------------------------------------------------
# Author: Jason Cruz
#...............................................................................
# Program: boxplot_m.R
# + First Created: 11/10/24
# + Last Updated: 12/15/24
#-------------------------------------------------------------------------------

#*******************************************************************************
# Libraries
#*******************************************************************************

library(RPostgres)
library(dplyr)
library(tidyr)
library(scales)
library(tcltk)

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
# Database Connection
#*******************************************************************************


#-------------------------------------------------------------------------------
# Import from PostgresSQL
#-------------------------------------------------------------------------------

#user <- Sys.getenv("CIUP_SQL_USER")
#password <- Sys.getenv("CIUP_SQL_PASS")
#host <- Sys.getenv("CIUP_SQL_HOST")
#port <- 5432
#database <- "gdp_revisions_datasets"

#con <- dbConnect(RPostgres::Postgres(), dbname = database, host = host, port = port, user = user, password = password)

#df <- dbGetQuery(con, "SELECT * FROM jefas_gdp_revisions_panel")

#dbDisconnect(con)


#-------------------------------------------------------------------------------
# Import from Excel or csv
#-------------------------------------------------------------------------------

# Load readxl package (install if needed)
if (!requireNamespace("readxl", quietly = TRUE)) {
  install.packages("readxl")
}
library(readxl)

# Import Excel file into R
r_gdp_revisions_panel <- read_excel(
  path = file.path(user_path, "jit_r_gdp_revisions_panel.xlsx")
)

# Quick check
cat("Data imported successfully. Dimensions:", 
    nrow(r_gdp_revisions_panel), "rows and", 
    ncol(r_gdp_revisions_panel), "columns.\n")



#*******************************************************************************
# Data Preparation
#*******************************************************************************

df <- r_gdp_revisions_panel %>% 
  filter(!is.na(horizon) & !is.na(target_period) & horizon >= 1 & horizon < 10)

df$horizon <- factor(df$horizon, levels = as.character(1:9))

# Rename "Rjit" to "e"
df <- df %>%
  rename(e = Rjit)

#*******************************************************************************
# Plotting Function
#*******************************************************************************

generate_boxplot <- function(data, variable, color, legend_position, output_dir) {
  output_file <- file.path(output_dir, paste0(variable, "Fig_Boxplot", ".png"))
  png(filename = output_file, width = 16, height = 9, units = "in", res = 300)  # +1 inch vertical para leyenda
  
  # Set plot margins and background
  par(bg = "white", mar = c(2.25, 4.75, 0.5, 0.5))  # Adjusted margins
  
  # Ensure 'horizon' is treated as a factor
  data$horizon <- factor(data$horizon, levels = 1:12)
  
  # Prepare the formula for plotting
  formula_str <- as.formula(paste0(variable, " ~ horizon"))
  
  # Create the boxplot without plotting (to gather stats)
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    plot = FALSE
  )
  
  # Calculate the min and max for the X axis (horizon)
  x_min <- min(bplt$group) - 0.35
  x_max <- max(bplt$group) + 0.35
  
  # Create an empty plot with Y-axis limits from -1.0 to 1.5
  plot(1, type = "n", xlim = c(x_min, x_max), ylim = c(-0.10, 0.2),
       xaxt = "n", yaxt = "n", xlab = "", ylab = "", bty = "n")
  
  # Define major ticks on Y-axis
  y_major_ticks <- seq(-0.10, 0.22, by = 0.04)  # Major ticks from -0.75 to 1.25 with 0.5 increments
  
  # Remove major and minor grid lines by commenting out these lines
  # Add horizontal grid lines for every 0.25 from -0.75 to 1.25 (minor grid)
  # y_minor_ticks <- seq(-0.75, 1.25, by = 0.25)
  # abline(h = y_minor_ticks, col = "#F5F5F5", lwd = 1.65, lty = 1)
  
  # Add major grid lines at -0.5, 0.0, 0.5, 1.0
  # y_major_ticks <- c(-0.5, 0.0, 0.5, 1.0)
  # abline(h = y_major_ticks, col = "#F5F5F5", lwd = 2, lty = 1)
  
  # Add vertical grid lines (minor grid lines)
  # x_minor_ticks <- seq(x_min, x_max, by = 0.5)
  # abline(v = x_minor_ticks, col = "#F5F5F5", lwd = 1.65, lty = 1)
  
  # Draw the boxplot with transparency
  col_alpha <- adjustcolor(color, alpha.f = 1)
  bplt <- boxplot(
    formula = formula_str,
    data = data,
    outline = FALSE,
    xlab = NULL,
    ylab = NULL,
    col = col_alpha,
    border = "#292929",
    lwd = 2.70,
    cex.axis = 1.75,
    cex.lab = 1.75,
    xaxt = "n",
    yaxt = "n",
    add = TRUE
  )
  
  # Add X-axis labels (horizon values)
  axis(1, at = seq_along(bplt$names), labels = bplt$names, cex.axis = 1.70)
  
  # Add Y-axis labels with ticks from -1.0 to 1.5 at 0.5 intervals
  axis(2, at = y_major_ticks, labels = sprintf("%.2f", y_major_ticks), cex.axis = 1.70, las = 1)
  
  # Calculate and add mean points
  means <- tapply(data[[variable]], data$horizon, mean, na.rm = TRUE)  # Calculate mean for each horizon
  
  # Plot the mean points in the middle of the boxes
  points(seq_along(means), means, col = color, pch = 21, cex = 3.35, bg = "#292929", lwd = 2.0)
  
  # Add the box around the plot
  box(lwd = 1.5)
  
  # Add the legend inside the plot, top-right
  legend("topright", inset = c(0.016, 0.03), legend = "Average", col = color,
         pch = 21, pt.cex = 3.15, cex = 1.55, pt.bg = "#292929",
         text.col = "black", horiz = TRUE, bty = "o", pt.lwd = 2.0,
         box.lwd = 1.5, xpd = TRUE, x.intersp = 0.6, y.intersp = 0.4,
         bg = "white")
  
  dev.off()
}





#*******************************************************************************
# Generate Plot for e
#*******************************************************************************

df_filtered_e <- df %>% 
  filter(!is.na(.data$e))  # Mantiene horizon = 1 en e

cat("Generating plots for variable e\n")
generate_boxplot(df_filtered_e, "r", "#3366FF", "topright", output_dir)

cat("All plots have been generated successfully in:", output_dir, "\n")



################################################################################
# Alternative: using ggplot
################################################################################

library(ggplot2)
library(dplyr)

# Ensure variables are correct
df_plot <- df %>%
  mutate(
    horizon = as.numeric(horizon),
    e = as.numeric(e)
  ) %>%
  filter(horizon >= 1 & horizon <= 9)


  
  p <- ggplot(df_plot, aes(x = factor(horizon), y = e)) +
  geom_boxplot(
    fill = "white",
    colour = "#3366FF",
    alpha = 1,
    outlier.shape = NA,   # removes outlier points
    width = 0.7,
    linewidth = 1.2
  ) +
  
  # Mean point with legend
  stat_summary(
    fun = mean,
    geom = "point",
    aes(shape = "Average"),   # map shape to label -> creates legend
    size = 6.5,
    stroke = 1.75,            # thicker border for mean point
    color = "white",
    fill = "#3366FF"
  ) +
  
  # Manual legend for shape
  scale_shape_manual(values = c("Average" = 21)) +
  
  labs(x = NULL, y = NULL, title = NULL, shape = NULL) +  # shape legend has no title
  
  scale_x_discrete(
    breaks = 1:10,
    labels = 1:10,
    expand = c(0.05, 0.05)
  ) +
  
  scale_y_continuous(
    breaks = seq(-0.10, 0.22, by = 0.04),
    labels = scales::number_format(accuracy = 0.01)
  ) +
  
  theme_minimal() +
  theme(
    panel.grid.major = element_blank(),
    panel.grid.minor.x = element_blank(),
    panel.grid.minor.y = element_blank(),
    axis.text = element_text(color = "black", size = 20),
    axis.text.x = element_text(color = "black", angle = 0, hjust = 0.5, vjust = 0.5),
    axis.text.y = element_text(color = "black", angle = 0, hjust = 0.5),
    axis.ticks = element_line(color = "black"),
    axis.ticks.length = unit(0.1, "inches"),
    axis.title.x = element_blank(),
    axis.title.y = element_text(size = 20, color = "black"),
    plot.title = element_blank(),
    
    # Legend inside top-right
    legend.position = c(0.97, 0.97),
    legend.justification = c("right", "top"),
    legend.text = element_text(size = 20, color = "black"),
    legend.background = element_rect(fill = "white", color = "black", linewidth = 0.45),
    
    axis.line = element_line(color = "black", linewidth = 0.45),
    panel.border = element_rect(color = "black", linewidth = 0.45, fill = NA),
    plot.margin = margin(9, 10, 9, 4)
  ) +
  
  coord_cartesian(ylim = c(-0.10, 0.22), clip = "off")

print(p)

# Save as PNG
ggsave(
  filename = file.path(output_dir, "rFig_Box1.png"), 
  plot = p, 
  width = 10, 
  height = 10, 
  dpi = 300,
  bg = "white"
)

# Save as high-resolution PDF
ggsave(filename = file.path(output_dir, "Fig_Box.pdf"), 
       plot = plot, 
       width = 16, 
       height = 9)

# Export EPS
ggsave(
  filename = file.path(output_dir, "Fig_Box.eps"), 
  plot = p, 
  width = 16, 
  height = 9, 
  device = "eps", 
  dpi = 300,
  bg = "white"
)


