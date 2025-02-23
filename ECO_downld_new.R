##not run create path
#for (i in 1:nrow(roi)){
 # dir.create(file.path("D:/ECO",paste0(roi$name[i],"/",roi$location[i])))

#}

##load libraries
library(appeears)
library(sf)
#set main working directory
pt<-"C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/test/ECOraw/"

# set a key to the keychain
##it needs an account set up in Earthdata (https://www.earthdata.nasa.gov/)
options(keyring_backend = "file")
rs_set_key(
  user = "abdullahusmani1",
  password = "haziqLOVERS123!"
)
#get token
token <- rs_login(user = "abdullahusmani1")
products <- rs_products()

layers <- rs_layers(
  product = "ECO_L2T_LSTE.002"
)

##dates 
##to be updated daily use: ed<-Sys.time(); sd<-Sys.time()-60*60*24
sd<-"2023-08-01"
ed<-"2023-09-16"


##df will include: LST, LST_err, cloud, Quality flags
df <- data.frame(
  task = "time_series",
  subtask = "subtask",
  start = sd,
  end = ed,
  product = c("ECO_L2T_LSTE.002"),
  layer = as.character(c(layers[c(1:7),11]))
)
#df<-rbind(df, c("time_series","subtask", sd, ed, "ECO_L2T_LSTE.002"))#, "SDS_CloudMask"))


##area of interest
roi<-st_read("C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/polygon/test/site_full_ext_Test.shp")


for (i in 1: nrow(roi)){
  roi2<-st_as_sf(st_as_sfc(st_bbox(roi[1,])))
#initializing task  
  df$task <- "polygon"
  task <- rs_build_task(
   df = df,
   roi = roi2,
   format = "geotiff"
  )
#download locally
  rs_request(
   request = task,
   user = "abdullahusmani1",
   transfer = TRUE,
   path = paste0(pt, roi2$name, roi2$location, "/" ),
   verbose = TRUE
  )

fs<-list.files(paste0(paste0(pt)), full.names = T)
}

dr_sub[k]


##############Data adjust and filtering

#produce multi-layer GeoTiff
library(terra)
pt<-"C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/test/ECOraw/"
ptout<-"C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/test/ECO/"
roi<-st_read("C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/polygon/test/site_full_ext_Test.shp")

dr<-list.dirs(pt, full.names = F, recursive = F)
for (i in 1:length(dr)){
  dr_sub<-list.dirs(paste0(pt,dr[i]),full.names = F, recursive = F)
  for(k in 1:length(dr_sub)){
    fl<-list.files(paste0(pt,dr[i],"/",dr_sub[k],"/" ), full.names = T)
    fl<-fl[which(grepl(".tif",fl,fixed=T))]
    dt<-unique(sub('_aid.*', '', sub('.*doy', '', fl)))
    ly<-unique(sub('_doy.*', '', sub('.*002_', '', fl)))#might add a check based on ly, to verify if all 7 layers are present.
    prj<-unique(sub('.tif.*', '', sub('.*01_', '', fl)))
    for (j in 1:length(dt)){
      a<-fl[which(grepl(dt[j],fl,fixed=T))]
      LST<-rast(a[which(grepl("LST_doy",a,fixed=T))])
      LST_err<-rast(a[which(grepl("LST_err",a,fixed=T))])
      QC<-rast(a[which(grepl("QC",a,fixed=T))])
      wt<-rast(a[which(grepl("water",a,fixed=T))])
      cl<-rast(a[which(grepl("cloud",a,fixed=T))])
      emis<-rast(a[which(grepl("Emis",a,fixed=T))])
      heig<-rast(a[which(grepl("height",a,fixed=T))])
      b<-c(LST,LST_err,QC,wt,cl,emis,heig)
      writeRaster(b, paste0(ptout, dr[i], dr_sub[k], "/",dr[i],"_",dr_sub[k], "_", dt[j], "_", prj, "_raw.tif" ))
      bdf<-as.data.frame(b, xy=T, na.rm=F)
      colnames(bdf)<-c("x","y","LST","LST_err","QC","wt","cloud","emis","height")
      write.csv(bdf, paste0(ptout, dr[i], dr_sub[k], "/",dr[i],"_",dr_sub[k], "_", dt[j], "_", prj, "_raw.csv" ))
      
      ###filtering
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
      # bdf$LST_filter<-ifelse(bdf$wt==0, NA, bdf$LST_filter)
      # bdf$LST_err_filter<-ifelse(bdf$wt==0, NA, bdf$LST_err_filter)
      # bdf$QC_filter<-ifelse(bdf$wt==0, NA, bdf$QC_filter)
      # bdf$emis_filter<-ifelse(bdf$wt==0, NA, bdf$emis_filter)
      # bdf$heig_filter<-ifelse(bdf$wt==0, NA, bdf$heig_filter)
      
      write.csv(bdf, paste0(ptout, dr[i], dr_sub[k], "/",dr[i],"_",dr_sub[k], "_", dt[j], "_", prj, "_filter.csv" ))
      ##might consider to remove all NA lines of bdf for optimizing storage space
      
      #rebuild .tif file
      LST_filt<-rast(matrix(bdf$LST_filter,ncol=ncol(LST), byrow = T))
      LST_err_filt<-rast(matrix(bdf$LST_err_filter,ncol=ncol(LST), byrow = T))
      LST_QC_filt<-rast(matrix(bdf$QC_filter,ncol=ncol(LST), byrow = T))
      LST_emis_filt<-rast(matrix(bdf$emis_filter,ncol=ncol(LST), byrow = T))
      LST_heig_filt<-rast(matrix(bdf$heig_filter,ncol=ncol(LST), byrow = T))
      b2<-c(LST_filt,LST_err_filt,LST_QC_filt,LST_emis_filt,LST_heig_filt)
      crs(b2)<-crs(b)
      ext(b2)<-ext(b)
      writeRaster(b2, paste0(ptout, dr[i], dr_sub[k], "/",dr[i],"_",dr_sub[k], "_", dt[j], "_", prj, "_filter.tif" ))
    }
  }
}

writeRaster(b2, "C:/Users/abdul/Documents/Uni/y2/2019 (SEGP)/Water Temp Sensors/test/ECO/multi.tif", overwrite=T)
