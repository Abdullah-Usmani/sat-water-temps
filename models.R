####load the  trainig data

script_dir <- dirname(normalizePath(sys.frame(1)$ofile))
land_path <- file.path(script_dir, "land.csv")
water_path <- file.path(script_dir, "water.csv")
if (!file.exists(land_path) || !file.exists(water_path)) {
  stop(paste0("Missing training CSV(s). Expected at: ", land_path, " and ", water_path))
}

land<-read.csv(land_path)
land$water<-0
water<-read.csv(water_path)
water$water<-1

tr<-rbind(land, water)

##implement model GAM
library(mgcv)
#compuite NDVI
tr$NDVI<-(B8-B4)/(B8+B4)
tr$NDWI<-(B03 - B08) / (B03 + B08)


g1<-gam(water~s(B2,B3,B4)+s(B8)+s(NDVI)+s(NDWI)+s(VV), data=tr, family = "binomial")

##the following model can be used as a backup in case you do not have cloud free Sentinel 2 images close to the ECOSTRESS/Landsat observation
g2<-gam(water~s(VV), data=tr, family = "binomial")


##model XGBoost
library(xgboost)
Yy<-as.matrix(tr[,c(4:14)])
Yy <- apply(Yy, 2, as.numeric)

xgb1<-xgboost(data=Yy, label = tr$water,objective="binary:logistic", nthread=5,  nround=20, max_depth=10)



##NOTE: in both cases you will get (after prediction) a matrix of probabilities. I would coinsider as water all pixel with probabilites >0.5 (we can adjust it in case later)
