# Load required libraries
library(terra)
library(sf)
library(mgcv)

script_dir <- dirname(normalizePath(sys.frame(1)$ofile))
land_path <- file.path(script_dir, "land.csv")
water_path <- file.path(script_dir, "water.csv")

if (!file.exists(land_path) | !file.exists(water_path)) {
  stop(paste0("Missing training CSV(s). Expected at: ", land_path, " and ", water_path))
}

land <- read.csv(land_path)
land$water <- 0
water <- read.csv(water_path)
water$water <- 1
tr <- rbind(land, water)
tr$NDVI <- (tr$B8 - tr$B4) / (tr$B8 + tr$B4)
tr$NDWI <- (tr$B03 - tr$B08) / (tr$B03 + tr$B08)

g1 <- gam(water ~ s(B2,B3,B4) + s(B8) + s(NDVI) + s(NDWI) + s(VV), data = tr, family = "binomial")

args <- commandArgs(trailingOnly=TRUE)
input_tiff <- args[1]
output_dir <- args[2]
ref_crs <- if (args[3] == "" || args[3] == "NULL") NULL else args[3]
AOI_path <- args[4]



# Output: wetted_raster01.tiff will be saved in output_dir

wet_area <- function(wt_path, input_paths, NAval=0, out_path, AOI_path=ROI_path, groud_truth=NULL, respl="low", classID=c("w", "t"), ref_crs=NA, xy=T, nth=4, form=NULL, gam_out=F,
                    class_prob=c(0.5, 0.5), main_water=NA, buffer=NA, full=F, geogr=F, aggregating_factor=NA) {
  if (require(terra) == F | require(sf) == F | require(mgcv) == F | require(stars) == F) {
    stop("install required packages (terra, sf, mgcv, stars)!")
  } else {
    message("reading input files and checking projection systems...")
    ly <- list()
    if (is.na(aggregating_factor)) {
      for (i in 1:length(input_paths)) {
        la <- rast(input_paths[i])
        NAflag(la) <- NAval
        ly[[i]] <- la
      }
    } else {
      for (i in 1:length(input_paths)) {
        la <- rast(input_paths[i])
        la <- rectify(la)
        NAflag(la) <- NAval
        la2 <- terra::aggregate(la, fact=aggregating_factor)
        NAflag(la2) <- NAval
        ly[[i]] <- la2
      }
    }
  }

  # Check the composition of reference system
  tab_crs <- data.frame(epsg=rep(NA, length(ly)), GEOG=rep(NA, length(ly)), PROJ=rep(NA, length(ly)))
  for (i in 1:length(ly)) {
    str <- unlist(strsplit(crs(ly[[i]]), " "))
    str_N <- as.numeric(gsub("\\D", "", str[length(str)]))
    ll <- which(grepl("GEOG", str[1]) == T)
    pp <- which(grepl("PROJ", str[1]) == T)
    tab_crs[i, 1] <- str_N
    tab_crs[i, 2] <- ifelse(length(ll) == 0, 0, 1)
    tab_crs[i, 3] <- ifelse(length(pp) == 0, 0, 1)
  }

  # Setting the case
  case <- ifelse(mean(tab_crs$GEOG) == 1 & mean(tab_crs$epsg) == tab_crs$epsg[1], "A",
                ifelse(mean(tab_crs$PROJ) == 1 & mean(tab_crs$epsg) == tab_crs$epsg[1], "B",
                      ifelse(mean(tab_crs$GEOG) == 1 & mean(tab_crs$epsg) != tab_crs$epsg[1], "C", "D")))

  print(paste("Case:", case))
  print(paste("ref_crs:", ref_crs))
  print(paste("AOI_path:", AOI_path))
  print(paste("is.null(ref_crs):", (is.null(ref_crs) | is.na(ref_crs))))
  print(paste("is.null(AOI_path):", (is.null(AOI_path) | is.na(AOI_path))))
  print(paste("Type of ref_crs: ", typeof(ref_crs)))
  # removed stray debug line referencing undefined wt_polygon


  # Case A line 1, 3
  if (case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == T |
      case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) == tab_crs$epsg[1]) {
    warning(paste("GAM4water is working on Geographical reference system(epsg:", tab_crs$epsg[1], "). Result can be inaccurate!"))
    warning("You have not set an AOI, procedure can be long especially if you're working with large raster. Consider setting an AOI")
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case A line 2 and 2a
  if (case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == F |
      case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) == tab_crs$epsg[1]) {
    warning(paste("GAM4water is working on Geographical reference system(epsg:", tab_crs$epsg[1], "). Result can be inaccurate!"))
    AOI <- read_sf(AOI_path)
    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI...")
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case A line 4
  if (case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) != tab_crs$epsg[1]) {
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI and reproject to defined CRS...")
    AOI <- read_sf(AOI_path)
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
      ly[[i]] <- project(ly[[i]], ref_crs)
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    rf2 <- ly[[mnp]]

    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          b <- crop(ly[[i]], rf2)
          ly[[i]] <- b
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      rf <- ly[[rs]]
      for (i in 1:length(ly)) {
        a <- resample(ly[[i]], rf)
        ly[[i]] <- a
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      rf <- ly[[rs]]
      for (i in 1:length(ly)) {
        a <- resample(ly[[i]], rf)
        ly[[i]] <- a
      }
    }
    gc()
  }

  # Case A line 5
  if (case == "A" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) != tab_crs$epsg[1]) {
    # Cropping each layer of ly
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    warning("You have not set an AOI, the procedure can be long especially if you're working with large raster. Consider setting an AOI")
    message("reproject to defined CRS...")
    for (i in 1:length(ly)) {
      ly[[i]] <- project(ly[[i]], ref_crs)
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case B line 1, 3
  if (case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == T |
      case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) == tab_crs$epsg[1]) {
    message(paste("GAM4water is working on Projected reference system(epsg:", tab_crs$epsg[1], ")."))
    warning("You have not set an AOI, procedure can be long especially if you're working with large raster. Consider setting an AOI")
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case B line 2 and 2a
  if (case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == F |
      case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) == tab_crs$epsg[1]) {
    message(paste("GAM4water is working on Projected reference system(epsg:", tab_crs$epsg[1], ")."))
    AOI <- read_sf(AOI_path)
    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI...")
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }

    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case B line 4
  if (case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) != tab_crs$epsg[1]) {
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI and reproject to defined CRS...")
    AOI <- read_sf(AOI_path)
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
      ly[[i]] <- project(ly[[i]], ref_crs)
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }

    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case B line 5
  if (case == "B" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T & as.numeric(gsub("\\D", "", ref_crs[length(ref_crs)])) != tab_crs$epsg[1]) {
    # Cropping each layer of ly
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    warning("You have not set an AOI, the procedure can be long especially if you're working with large raster. Consider setting an AOI")
    message("reproject to defined CRS...")
    for (i in 1:length(ly)) {
      ly[[i]] <- project(ly[[i]], ref_crs)
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case C line 1 (line 3 does not exist in line C and D)
  if (case == "C" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == T) {
    warning(paste("All your raster have different Geographical reference systems. GAM4water will use first raster reference system (epsg:", tab_crs$epsg[1], "). Result can be inaccurate!"))
    warning("You have not set an AOI, procedure can be long especially if you're working with large raster. Consider setting an AOI")

    # Homogenizing projection
    ref_crs <- paste0("epsg:", tab_crs$epsg[1])

    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case C line 2
  if (case == "C" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == F) {
    warning(paste("All your raster have different Geographical reference systems. GAM4water will use first raster reference system (epsg:", tab_crs$epsg[1], "). Result can be inaccurate!"))

    # Homogenizing projection
    ref_crs <- paste0("epsg:", tab_crs$epsg[1])

    AOI <- read_sf(AOI_path)

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI...")
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }

    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case C line 4
  if (case == "C" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F) {
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI and reproject to defined CRS...")
    AOI <- read_sf(AOI_path)
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }
    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case C line 5
  if (case == "C" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T) {
    # Cropping each layer of ly
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    warning("You have not set an AOI, the procedure can be long especially if you're working with large raster. Consider setting an AOI")
    message("reproject to defined CRS...")
    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case D line 1 (line 3 does not exist in line C and D)
  if (case == "D" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == T) {
    message(paste("GAM4water is working on Projected reference system(epsg:", tab_crs$epsg[which.min(tab_crs$PROJ == 1)], ")."))
    warning("You have not set an AOI, procedure can be long especially if you're working with large raster. Consider setting an AOI")

    # Homogenizing projection
    ref_crs <- paste0("epsg:", tab_crs$epsg[which.min(tab_crs$PROJ == 1)])

    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }

    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Case D line 2
  if (case == "D" & (is.null(ref_crs) | is.na(ref_crs)) == T & (is.null(AOI_path) | is.na(AOI_path)) == F) {
    message(paste("GAM4water will use the first raster's projected reference system available (epsg:", tab_crs$epsg[which.min(tab_crs$PROJ == 1)], ")."))

    # Homogenizing projection
    ref_crs <- paste0("epsg:", tab_crs$epsg[which.min(tab_crs$PROJ == 1)])

    AOI <- read_sf(AOI_path)

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI...")
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }

    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }

    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case D line 4
  if (case == "D" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == F) {
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    # Cropping each layer of ly
    message("Cropping rasters to the defined AOI and reproject to defined CRS...")
    AOI <- read_sf(AOI_path)
    for (i in 1:length(ly)) {
      AOIr <- st_transform(AOI, crs(ly[[i]]))
      ly[[i]] <- mask(ly[[i]], AOIr)
    }
    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }

    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }
    gc()
  }

  # Case D line 5
  if (case == "D" & (is.null(ref_crs) | is.na(ref_crs)) == F & (is.null(AOI_path) | is.na(AOI_path)) == T) {
    # Cropping each layer of ly
    AA <- unlist(strsplit(crs(ref_crs), " "))
    ll <- which(grepl("GEOG", AA[1]) == T)
    if (length(ll) > 0) {
      warning(paste("You have set a Geographical reference system(", ref_crs, "). Result can be inaccurate!"))
    }

    warning("You have not set an AOI, the procedure can be long especially if you're working with large raster. Consider setting an AOI")
    message("reproject to defined CRS...")
    for (i in 1:length(ly)) {
      if (paste0("epsg:", tab_crs$epsg[i]) == ref_crs) {
        ly[[i]] <- ly[[i]]
      } else {
        ly[[i]] <- project(ly[[i]], ref_crs)
      }
    }
    # Check extents and eventually crop to the raster with smallest extent
    ex <- c()
    for (i in 1:length(ly)) {
      ex1 <- ext(ly[[i]])
      ex[i] <- (ex1[2] - ex1[1]) * (ex1[4] - ex1[3])
    }
    mn <- ex[which.min(ex)]
    mnp <- which.min(ex)
    TF <- ex == mn
    if (all(TF == T) == F) {
      message("matching extents...")
      for (i in 1:length(ly)) {
        if (TF[i] == T) {
          ly[i] <- ly[i]
        } else {
          ly[[i]] <- crop(ly[[i]], ly[[mnp]])
        }
      }
    }

    # Resample each raster to the highest or lower layer resolution
    rr <- c()
    for (i in 1:length(ly)) {
      r <- res(ly[[i]])
      r2 <- r[1] * r[2]
      rr[i] <- r2
    }

    if (respl == "low" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.max(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    } else if (respl == "high" & all(rr == rr[1]) == F) {
      message("homogenizing resolutions...")
      rs <- which.min(rr)
      for (i in 1:length(ly)) {
        ly[[i]] <- resample(ly[[i]], ly[[rs]])
      }
    }
    gc()
  }

  # Produce an actual multilayer raster
  ml <- rast()
  for (i in 1:length(ly)) {
    ml <- c(ml, ly[[i]])
  }

  # Reading the water/non_water polygon shp file
  message("Getting water and non-water polygons")
  p <- read_sf(wt_path)

  # Check if has the same projection system
  strP <- unlist(strsplit(crs(p), " "))
  strP_N <- as.numeric(gsub("\\D", "", strP[length(strP)]))
  strLY <- unlist(strsplit(crs(ly[[1]]), " "))
  strLY_N <- as.numeric(gsub("\\D", "", strLY[length(strLY)]))

  if (strP_N == strLY_N) {
    p <- p
  } else {
    p <- st_transform(p, strLY_N)
  }
  p$wt01 <- ifelse(p$wt == classID[1], 1, 0)  # Assign 0-1 code
  gc()

  # Create data frame with pixel radiometric values for each polygon
  s12_df <- data.frame()
  for (i in 1:nrow(p)) {
    ms <- p[i, ]
    rs <- crop(ml, ms)
    rs <- mask(rs, ms)
    s_df <- as.data.frame(rs, xy=T, na.rm=F)
    na <- which(complete.cases(s_df) == F)
    if (length(na) == 0 | length(na) == nrow(s_df)) {
      s_df <- s_df
    } else {
      s_df <- s_df[-na, ]
    }
    s_df$wt01 <- ifelse(ms$wt01 == 1, 1, 0)
    s12_df <- rbind(s12_df, s_df)
  }

  s12_df$row <- seq(1, nrow(s12_df))
  s12_df <- st_as_sf(s12_df, coords=c("x", "y"), remove=F)

  colnames(s12_df)[c(1:ncol(s12_df)-1)] <- c("x", "y", paste0("ly", seq(1, nlyr(ml))), "wt01", "row")
  s12_df$wt01 <- as.factor(s12_df$wt01)
  st_crs(s12_df) <- crs(ml)

  # Produce a training and validation datasets...validation based on pixel classification and not on an actual ground truth.
  if (is.null(groud_truth)) {
    message("Producing a training and validation dataset...")
    type <- c(levels(s12_df$wt01))
    p_tr <- st_sf(st_sfc())
    p_vl <- st_sf(st_sfc())
    st_crs(p_tr) <- ref_crs
    st_crs(p_vl) <- ref_crs

    for (i in 1:length(type)) {
      df <- s12_df[which(s12_df$wt01 == type[i]), ]
      df_tr <- df[sample(nrow(df), nrow(df) * (2/3)), ]
      df_vl <- df[-which(df$row %in% df_tr$row), ]
      p_tr <- rbind(p_tr, df_tr)
      p_vl <- rbind(p_vl, df_vl)
    }
  } else {
    p_tr <- s12_df
  }

  # Setting the model
  message("fitting the GAM4water model...")
  if (is.null(form)) {
    ts <- "ts"
    pred <- Reduce(paste, paste0("s(ly", seq(1, nlyr(ml)), ",bs=", ts, ")+"))

    if (xy == T) {
      f <- as.formula(paste("wt01~", pred, "te(x,y,bs=ts)"))
      gam_wt_CAT <- bam(f, data=p_tr, family=binomial, nthreads=nth, discrete=T)
    } else {
      pred <- substr(pred, 1, nchar(pred) - 1)
      f <- as.formula(paste("wt01~", pred))
      gam_wt_CAT <- bam(f, data=p_tr, family=binomial, nthreads=nth, discrete=T)
    }
  } else {
    f <- as.formula(paste("wt01~", form))
    gam_wt_CAT <- bam(f, data=p_tr, family=binomial, nthreads=nth, discrete=T)
  }
  gc()

  if (gam_out == T) {
    gamOut <- function(res, file="test.csv", ndigit=5, writecsv=T) {
      if (length(grep("summary", class(res))) == 0) res <- summary(res)
      co <- res$p.table  # Result table
      nvar <- nrow(co)  # No. row as in summary()$coefficients
      ncoll <- ncol(co)  # No. col as in summary()$coefficients

      formatter <- function(x) format(round(x, ndigit), nsmall=ndigit)
      nstats <- 4  # Sets the number of rows to record the coefficients

      # s table
      StartRow <- nstats + nvar + 2  # Starting row of for second table
      st <- res$s.table
      ncol2 <- ncol(st)
      nrow2 <- nrow(st)
      # Create a matrix
      G <- matrix("", nrow=(nvar + nstats + StartRow), ncol=(ncoll + 1))  # Storing data for output
      # Equation, deviance and R square
      G[1, 1] <- toString(res$formula)
      G[1, 2] <- "Deviance explained"  # Save AIC value
      G[2, 2] <- res$dev.expl
      G[1, 3] <- "R-sq. (adj)"
      G[2, 3] <- res$r.sq
      # P table
      G[(nstats + 1):(nvar + nstats), 1] <- rownames(co)  # Save rownames and colnames
      G[nstats, 2:(ncoll + 1)] <- colnames(co)
      G[(nstats + 1):(nvar + nstats), 2:(ncoll + 1)] <- formatter(co)  # Save coefficients
      # S table
      G[(StartRow + 1):(StartRow + nrow2), 1] <- rownames(st)
      G[(StartRow), 2:(ncol2 + 1)] <- colnames(st)
      G[(StartRow + 1):(StartRow + nrow2), 2:(ncol2 + 1)] <- formatter(st)

      # For output
      print(G)
      write.csv(G, file=file, row.names=F)
    }
    newdir <- paste0(out_path, "GAM")
    dir.create(newdir)
    gamOut(gam_wt_CAT, paste0(newdir, "/summary.csv"))
    jpeg(paste0(newsdir, "/predictors.jpeg"))
    par(mar=c(4.1, 4.4, 4.1, 1.9), xaxs="i", yaxs="i")
    plot(s, pages=2)
    dev.off()
  }

  r01df <- as.data.frame(ml, xy=T, na.rm=F)  # Transform multi-layer raster into a data frame format
  r01df$row <- rownames(r01df)
  colnames(r01df) <- c("x", "y", paste0("ly", seq(1, nlyr(ml))))
  gc()

  # Predict probability based on pixel information
  message("ALMOST THERE... predicting probabilities")
  r01df$wt_prd <- predict.gam(gam_wt_CAT, r01df, type="response")  # Predict class of each pixel

  # Attribute 0 or 1 based on probability thresholds
  r01df$wt_prd01 <- ifelse(r01df$wt_prd >= 0 & r01df$wt_prd <= class_prob[1], 0, 1)

  dwa_m <- matrix(r01df$wt_prd01, ncol=ncol(ml), byrow=T)
  dwa <- rast(dwa_m)
  ext(dwa) <- ext(ml)
  crs(dwa) <- crs(ml)
  plot(dwa)
  gc()

  # Produce shp wetted polygon from raster
  library(stars)
  wetted <- st_as_stars(dwa) %>% st_as_sf(merge=T)  # Produce an shp
  wetted$area <- as.numeric(st_area(wetted))

  maina <- wetted[wetted$lyr.1 == 1, ]

  # Extract solely the main water body or not
  message("Building binary raster, wetted area(s) shapefile and writing them...")
  if (is.na(main_water)) {
    main <- maina
  } else {
    main <- maina[which(maina$area > main_water), ]
  }

  # Apply a buffer if requested
  if (!is.na(buffer)) {
    main <- st_buffer(main, buffer)
  } else {
    main <- main
  }

  write_sf(main, paste0(out_path, "wetted_polygon.shp"))
  writeRaster(dwa, paste0(out_path, "wetted_raster01.tiff"), overwrite=T)
  gc()

  # If user wants geogr outputs
  if (geogr == T) {
    message("Re-projecting the output with the WGS84 projection system and writing them...")
    main84 <- st_transform(main, 4326)
    dwa84 <- project(dwa, "epsg:4326")
    write_sf(main84, paste0(out_path, "wetted_polygon_epsg4326.shp"))
    writeRaster(dwa84, paste0(out_path, "wetted_raster01_epsg4326.tiff"), overwrite=T)
  }

  # Write each layer cropped with wetted mask
  if (full == T & geogr == F) {
    message("Cropping and writing your input raster with the water area!")
    for (i in 1:length(ly)) {
      sd <- mask(ly[[i]], main)
      writeRaster(sd, paste0(out_path, "ly", i, "_wetted.tiff"), overwrite=T)
    }
  } else if (full == T & geogr == T) {
    message("Re-projecting the cropped input raster with the WGS84 projection system and write them...")
    for (i in 1:length(ly)) {
      sd <- mask(ly[[i]], main)
      sd84 <- project(sd, "epsg:4326")
      writeRaster(sd, paste0(out_path, "ly", i, "_wetted.tiff"), overwrite=T)
      writeRaster(sd84, paste0(out_path, "ly", i, "_wetted_epsg4326.tiff"), overwrite=T)
    }
  }
  gc()

  message("Classification performance ongoing!")
  if (is.null(groud_truth)) {
    # Classification validation
    pw <- p_vl[which(p_vl$wt01 == 1), ]
    pt <- p_vl[which(p_vl$wt01 == 0), ]
    pw <- st_zm(pw)
    pt <- st_zm(pt)
    w <- terra::extract(dwa, pw)
    t <- terra::extract(dwa, pt)
    # Compute True Positives, False negative, False Positives
    Tp <- nrow(w[which(w$lyr.1 == 1), ])
    Fn <- nrow(w[which(w$lyr.1 == 0), ]) + nrow(w[is.na(w$lyr.1), ])
    Fp <- nrow(t[which(t$lyr.1 == 1), ])
    P <- Tp / (Tp + Fp)  # Precision
    R <- Tp / (Tp + Fn)  # Recall
    F1 <- 2 * ((P * R) / (P + R))  # F1
    perf <- data.frame(tr_pixel=nrow(p_tr), val_pixel=nrow(p_vl), Tp=Tp, Fn=Fn, Fp=Fp, P=P, R=R, F1=F1)
  } else {
    gt <- read_sf(ground_truth)
    gt <- st_transform(gt, crs(ml))
    w <- st_as_sf(r01df, coords=c("x", "y"))
    st_crs(w) <- crs(ml)
    w$wtn <- st_within(w, gt)
    Tp <- nrow(w[which(w$wtn == T & w$wt_prd01 == 1), ])
    Fn <- nrow(w[which(w$wtn == T & w$wt_prd01 == 0), ]) + nrow(w[which(w$wtn == T & is.na(w$wt_prd01)), ])
    Fp <- nrow(t[which(w$wtn == F & w$wt_prd01 == 1), ])
    P <- Tp / (Tp + Fp)  # Precision
    R <- Tp / (Tp + Fn)  # Recall
    F1 <- 2 * ((P * R) / (P + R))  # F1
    perf <- data.frame(tr_pixel=nrow(p_tr), val_pixel=nrow(p_vl), Tp=Tp, Fn=Fn, Fp=Fp, P=P, R=R, F1=F1)
  }
  write.csv(perf, paste0(out_path, "class_performance.csv"))
  message("ALL DONE!!")
}

# Run GAM4water classification
wet_area(
  input_paths = input_tiff,
  out_path = output_dir,
  AOI_path = AOI_path,
  ref_crs = ref_crs,
  classID = c("w", "t"),
  respl = "low"
)

