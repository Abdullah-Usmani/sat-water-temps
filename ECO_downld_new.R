library(appeears)
library(sf)
library(terra)

# Set up directories
base_path_raw <- "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECOraw\\"
base_path_out <- "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECO\\"

# Read the ROI shapefile
roi <- st_read("C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\polygon\\magat\\magat_shape.shp")

# Ensure directories exist
for (i in 1:nrow(roi)) {
    dir_path_raw <- file.path(base_path_raw, roi$name[i], roi$location[i])
    dir_path_out <- file.path(base_path_out, roi$name[i], roi$location[i])

    if (!dir.exists(dir_path_raw)) dir.create(dir_path_raw, recursive = TRUE)
    if (!dir.exists(dir_path_out)) dir.create(dir_path_out, recursive = TRUE)
}

# Set up authentication
options(keyring_backend = "file")
rs_set_key(user = "abdullahusmani1", password = "haziqLOVERS123!")
token <- rs_login(user = "abdullahusmani1")

# # Get available products
# products <- rs_products()
# layers <- rs_layers(product = "ECO_L2T_LSTE.002")

# # Define date range
# sd <- "2023-08-01"
# ed <- "2023-09-16"

# # Define data frame for task submission
# df <- data.frame(
#   task = "time_series",
#   subtask = "subtask",
#   start = sd,
#   end = ed,
#   product = "ECO_L2T_LSTE.002",
#   layer = as.character(layers[c(1:7), 11])
# )

# # Iterate through all polygons
# for (i in 1:nrow(roi)) {
#     roi2 <- st_as_sf(st_as_sfc(st_bbox(roi[i,])))

#     df$task <- "polygon"
#     task <- rs_build_task(
#         df = df,
#         roi = roi2,
#         format = "geotiff"
#     )

#     # Ensure proper path
#     download_path <- file.path(base_path_raw, roi$name[i], roi$location[i])
    
#     rs_request(
#         request = task,
#         user = "abdullahusmani1",
#         transfer = TRUE,
#         path = download_path,  # Corrected path
#         verbose = TRUE
#     )
# }

# Processing downloaded data
# Define input and output directories
input_dirs <- c(
  "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECOraw\\Magat/lake/polygon/",
  "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECOraw\\Magat/river/polygon/"
)

output_dirs <- c(
  "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECO\\Magat/lake/polygon/",
  "C:\\Users\\abdul\\Documents\\Uni\\y2\\2019 (SEGP)\\Water Temp Sensors\\test\\ECO\\Magat/river/polygon/"
)

# Iterate over input folders
for (i in seq_along(input_dirs)) {
  input_folder <- normalizePath(input_dirs[i], winslash = "/")
  output_folder <- normalizePath(output_dirs[i], winslash = "/")

  # Ensure output directory exists
  if (!dir.exists(output_folder)) {
    dir.create(output_folder, recursive = TRUE)
  }

  # List all .tif files in the folder
  tif_files <- list.files(input_folder, pattern = "\\.tif$", recursive = TRUE, full.names = TRUE)

  if (length(tif_files) == 0) {
    print(paste("No .tif files found in:", input_folder))
    next
  }

  print(paste("Processing", length(tif_files), "files in:", input_folder))

  # Group files by doy
  file_groups <- split(tif_files, sub(".*doy(\\d+)_.*", "\\1", tif_files))

  for (doy in names(file_groups)) {
    files <- file_groups[[doy]]

    # Extract metadata
    prj <- sub('.tif.*', '', sub('.*01_', '', files[1]))
    
    # Identify required rasters
    LST_file <- files[grepl("LST_doy", files, fixed = TRUE)]
    LST_err_file <- files[grepl("LST_err", files, fixed = TRUE)]
    QC_file <- files[grepl("QC", files, fixed = TRUE)]
    wt_file <- files[grepl("water", files, fixed = TRUE)]
    cl_file <- files[grepl("cloud", files, fixed = TRUE)]
    emis_file <- files[grepl("Emis", files, fixed = TRUE)]
    heig_file <- files[grepl("height", files, fixed = TRUE)]

    # Ensure required files exist
    if (length(LST_file) == 0 || length(QC_file) == 0) {
      print(paste("Skipping doy", doy, "due to missing required files"))
      next
    }

    # Load rasters
    LST <- if (length(LST_file) > 0) rast(LST_file) else NULL
    LST_err <- if (length(LST_err_file) > 0) rast(LST_err_file) else NULL
    QC <- if (length(QC_file) > 0) rast(QC_file) else NULL
    wt <- if (length(wt_file) > 0) rast(wt_file) else NULL
    cl <- if (length(cl_file) > 0) rast(cl_file) else NULL
    emis <- if (length(emis_file) > 0) rast(emis_file) else NULL
    heig <- if (length(heig_file) > 0) rast(heig_file) else NULL

    # Combine available rasters
    b <- c(LST, LST_err, QC, wt, cl, emis, heig)

    # Save raw raster
    raw_output_path <- file.path(output_folder, paste0("processed_", doy, "_", prj, "_raw.tif"))
    writeRaster(b, raw_output_path, overwrite=TRUE)

    # Convert raster to dataframe
    bdf <- as.data.frame(b, xy = TRUE, na.rm = FALSE)
    colnames(bdf) <- c("x", "y", "LST", "LST_err", "QC", "wt", "cloud", "emis", "height")

    csv_output_path <- file.path(output_folder, paste0("processed_", doy, "_", prj, "_raw.csv"))
    write.csv(bdf, csv_output_path)

    # Apply filtering conditions
      ##quality control filter(QU)
      bdf$LST_filter<-ifelse(bdf$QC%in%c(15, 2501, 3525, 65535), NA, bdf$LST)
      bdf$LST_err_filter<-ifelse(bdf$QC%in%c(15, 2501, 3525, 65535), NA, bdf$LST_err)
      bdf$QC_filter<-ifelse(bdf$QC%in%c(15, 2501, 3525, 65535), NA, bdf$QC)
      bdf$emis_filter<-ifelse(bdf$QC%in%c(15, 2501, 3525, 65535), NA, bdf$emis)
      bdf$heig_filter<-ifelse(bdf$QC%in%c(15, 2501, 3525, 65535), NA, bdf$height)
      
      
      ##cloud filter
      bdf$LST_filter<-ifelse(bdf$cloud==1, NA, bdf$LST_filter)
      bdf$LST_err_filter<-ifelse(bdf$cloud==1, NA, bdf$LST_err_filter)
      bdf$QC_filter<-ifelse(bdf$cloud==1, NA, bdf$QC_filter)
      bdf$emis_filter<-ifelse(bdf$cloud==1, NA, bdf$emis_filter)
      bdf$heig_filter<-ifelse(bdf$cloud==1, NA, bdf$heig_filter)
      
      ##NEW!!
      #water filter
      bdf$LST_filter<-ifelse(bdf$wt==0, NA, bdf$LST_filter)
      bdf$LST_err_filter<-ifelse(bdf$wt==0, NA, bdf$LST_err_filter)
      bdf$QC_filter<-ifelse(bdf$wt==0, NA, bdf$QC_filter)
      bdf$emis_filter<-ifelse(bdf$wt==0, NA, bdf$emis_filter)
      bdf$heig_filter<-ifelse(bdf$wt==0, NA, bdf$heig_filter)

    filtered_csv_output_path <- file.path(output_folder, paste0("processed_", doy, "_", prj, "_filtered.csv"))
    write.csv(bdf, filtered_csv_output_path)

    # Rebuild filtered multi-band raster
    LST_filt <- rast(matrix(bdf$LST_filter, ncol = ncol(LST), byrow = TRUE))
    LST_err_filt <- rast(matrix(bdf$LST_err_filter, ncol = ncol(LST), byrow = TRUE))
    QC_filt <- rast(matrix(bdf$QC_filter, ncol = ncol(LST), byrow = TRUE))
    emis_filt <- rast(matrix(bdf$emis_filter, ncol = ncol(LST), byrow = TRUE))
    heig_filt <- rast(matrix(bdf$height, ncol = ncol(LST), byrow = TRUE))
    
    b2 <- c(LST_filt, LST_err_filt, QC_filt, emis_filt, heig_filt)
    crs(b2) <- crs(b)
    ext(b2) <- ext(b)

    multi_tif_output_path <- file.path(output_folder, paste0("processed_", doy, "_", prj, "_filtered.tif"))
    writeRaster(b2, multi_tif_output_path)

    # Save multi-band raster
    multi_tif_output_path <- file.path(output_folder, paste0("processed_", doy, "_", prj, "_multi.tif"))
    writeRaster(b, multi_tif_output_path, overwrite=TRUE)

    print(paste("Processed data for doy:", doy))
  }
}

print("Processing complete!")
