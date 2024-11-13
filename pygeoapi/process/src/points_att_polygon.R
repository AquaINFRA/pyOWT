############################################################################################.
## 1. points_att_polygon ####
# function points_att_polygon - data points merged with polygon attributes based on data point location

library(sf)
library(magrittr)
library(dplyr)
library(janitor)
library(sp)
library(data.table)

sessionInfo()

points_att_polygon <- function(shp, dpoints, long_col_name="long", lat_col_name="lat") {
  #shp - shapefile
  #dpoints - dataframe with values and numeric variables for coordinates:
  #long - longitude column name in dpoints; default "long"
  #lat - latitude column name in dpoints; default "lat"

  if (!requireNamespace("sp", quietly = TRUE)) {
    stop("Package \"sp\" must be installed to use this function.",
         call. = FALSE)
  }
  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("Package \"sf\" must be installed to use this function.",
         call. = FALSE)
  }

  if (missing(shp))
    stop("missing shp")
  if (missing(dpoints))
    stop("missing dpoints")

  #dpoints to spatial
  print('Making input data spatial based on long, lat...')
  data_spatial <- sf::st_as_sf(dpoints, coords = c(long_col_name, lat_col_name))
  # set to WGS84 projection
  print('Setting to WGS84 CRS...')
  sf::st_crs(data_spatial) <- 4326
  print('It may take a while...')
  
  shp_wgs84 <- st_transform(shp, st_crs(data_spatial))
  if (!all(st_is_valid(shp_wgs84))) {
    shp_wgs84 <- st_make_valid(shp_wgs84)
  }

  shp_wgs84 <- st_filter(shp_wgs84, data_spatial) 
  data_shp <- st_join(shp_wgs84, data_spatial)
  data_shp <- sf::st_drop_geometry(data_shp)
  res <- full_join(dpoints, data_shp)
  rm(data_spatial)
  print('Done!')
  res
}

# Retrieve command line arguments
args <- commandArgs(trailingOnly = TRUE)
print(paste0('R Command line args: ', args))
in_shp_url <- args[1]
in_dpoints_url <- args[2]
in_long_col_name <- args[3]
in_lat_col_name <- args[4]
out_result_path <- args[5]

shapefile <- st_read(in_shp_url)

# Read excel or CSV file
# load in situ data and respective metadata (geolocation and date are mandatory metadata)
# from DDAS: https://vm4412.kaj.pouta.csc.fi/ddas/oapif/collections/lva_secchi/items?f=csv&limit=10000
# in_situ_data/in_situ_example.xlsx : example data from https://latmare.lhei.lv/
# in_situ_data/Latmare_20240111_secchi_color.xlsx : # data from LIAE data base from https://latmare.lhei.lv/
read_data <- function(table_file_path) {
  data_raw <- tryCatch(
    {
      data_raw <- NULL

      if (grepl("f=csv", table_file_path) | grepl("\\.csv$", table_file_path)) {
        data_raw <- read.csv(table_file_path) %>%
          janitor::clean_names()
        print(paste0("CSV file ", table_file_path, " read"))
      } else if (grepl("f=json", table_file_path) | grepl("\\.json$", table_file_path)) {
        data_raw <- st_read(table_file_path) %>%
          janitor::clean_names()
        print(paste0("GeoJSON file ", table_file_path, " read"))
      } else if (grepl("\\.xlsx$", table_file_path)) {
        data_raw <- readxl::read_excel(table_file_path) %>%
          janitor::clean_names()
        print(paste0("Excel file ", table_file_path, " read"))
      } else {
        stop("Unsupported file format: only CSV, JSON, or Excel accepted.")
      }

      if (!is.null(data_raw)) {
        if ("transparen" %in% colnames(data_raw)) {
          colnames(data_raw)[colnames(data_raw) == "transparen"] <- "transparency_m"
        }
        return(data_raw)
      } else {
        stop("data_raw is NULL: No data read.")
      }
    },
    error = function(err) {
      print(paste("Error:", err$message))
      return(NULL)
    }
  )
  return(data_raw)
}

data_raw <- read_data(in_dpoints_url)

if (is.null(data_raw)) {
  print("Data reading failed or no valid data.")
} else {
  print("Data read successfully.")
}


# list relevant columns: geolocation (lat and lon), date and values for data points are mandatory
rel_columns <- c(
  "longitude",
  "latitude",
  "visit_date",
  "transparency_m",
  "color_id" #water color hue in Furel-Ule (categories)
)

data_rel <- data_raw %>%
  dplyr::select(all_of(rel_columns)) %>%
  # remove cases when Secchi depth, water colour were not measured
  filter(
    !is.na(`transparency_m`) &
      !is.na(`color_id`) &
      !is.na(`longitude`) &
      !is.na(`latitude`)
  )

# set coordinates ad numeric (in case they are read as chr variables)
data_rel <- data_rel %>%
  mutate(
    longitude  = as.numeric(longitude),
    latitude   = as.numeric(latitude),
    transparency_m = as.numeric(transparency_m)
  )

# Run the function "points_att_polygon"
out_points_att_polygon <- points_att_polygon(shp = shapefile,
                                             dpoints = data_rel,
                                             long_col_name = in_long_col_name,
                                             lat_col_name = in_lat_col_name)

# Write the result to csv file:
print(paste0('Write result to csv file: ', out_result_path))
data.table::fwrite(out_points_att_polygon, file = out_result_path)