#Think about breaking this into two parts: one script for gathering the catch/haul data (the .csvs) and then the subseqent manipulation performed on them?



# Query EcoDAAT for most recent larval catch data
# Combine hauls and catch to get catch w zeros.

## First, get QCd data from database. First query specimen data for CPUE, then query haul data for all hauls 
## (merge to create "catch with zero"). Bongo and Tucker data are queried separately. (Only 2 cruises with Tucker data to be used.)
## For a new year, make sure to update the query to include most recent cruise.

require(RODBC); require(here)
library(lubridate)

#Query EcoDAAT for update to date larval catch data----

{user <- readline("Input Username: ")
pswd <- readline("Input Password: ")}

AFSCconnect <- odbcConnect("AFSC", uid=user,  pwd=pswd)

#sqlQuery(AFSCconnect,"DROP TABLE SPECIMEN_MAIN_GEOM;")
#sqlQuery(AFSCconnect,"CREATE TABLE SPECIMEN_MAIN_GEOM AS SELECT * FROM ECODAAT.SPECIMEN_MAIN_GEOM;")
#sqlQuery(AFSCconnect,"DROP TABLE HAUL;")
#sqlQuery(AFSCconnect,"CREATE TABLE HAUL AS SELECT * FROM ECODAAT.HAUL;")

# Query from ECODAAT_DEV to get most recent 2023 verified data.

BonSampleQuery <-
  paste0(
    "SELECT 
    COMMENTS_SPECIMEN, COMMON_NAME_ICHBASE, CRUISE, GMT_DATE_TIME, HAUL_ID, LARVALCATCHPER1000M3, 
    LARVALCATCHPER10M2,LAT,LON, NET, NUMBER_CAUGHT, POLYGONAL_AREA, PRIMARY_NET, PURPOSE, SPECIES_NAME, STATION_NAME, YEAR 
  FROM ECODAAT_DEV.SPECIMEN_MAIN_GEOM 
  WHERE GEAR_NAME LIKE '60BON' AND
    STAGE_ID=6 AND    
    SPECIES_NAME IN ('Ammodytes personatus','Atheresthes stomias','Bathymaster spp.','Gadus chalcogrammus',
                    'Gadus macrocephalus','Hippoglossoides elassodon','Hippoglossus stenolepis','Lepidopsetta bilineata',
                    'Lepidopsetta polyxystra','Platichthys stellatus','Sebastes spp.','Stenobrachius leucopsarus') AND
    PRIMARY_NET IN('Y') AND
    HAUL_PERFORMANCE IN('GOOD','QUEST') AND
    PURPOSE IN('GRID') AND
    CRUISE IN('3SH81', '4MF81', '2DA82', '1CH83', '2PO85', '3MF87', '4MF90', '4MF91', '4MF92', '5MF93', '6MF94', '8MF95', '8MF96', '8MF97',
    '5MF98', '2WE99', '5MF99', '6MF00', '3MF01', '4MF02', '5MF03', '5MF04', '6MF05', '4MF06', '5MF07', '4DY08', '4DY09', '3DY10',
     '2DY11', 'DY13-06', 'DY15-05', 'DY17-05', 'DY19-05', 'WO21-01','DY23-07')"
  )

samples.b<-sqlQuery(AFSCconnect,BonSampleQuery) 
#dim(samples.b) # on 5/9/2022 gave 19068 values, as opposed to 19066 in saved data file (EcoDAAT) from last year. 20708 on 1/12/23, 20718 on 1/13/23
# 21276 on 8/7/2023
# 21525 on 9/24/2024 (21272 not included the added DY23-07 samples)
BonHaulQuery <-
  paste0(
    "SELECT 
      COMMENTS_HAUL, CRUISE, DAY, GEAR_NAME, GMT_DATE_TIME, HAUL_ID, HAUL_NAME,       
      HAUL_PERFORMANCE, LAT, LON, MAX_GEAR_DEPTH, MESH, MONTH, NET,           
      POLYGONAL_AREA, PRIMARY_NET, PURPOSE,  STATION_NAME, YEAR    
   FROM ECODAAT_DEV.HAUL 
   WHERE GEAR_NAME LIKE '60BON' AND
    PRIMARY_NET IN('Y') AND
    HAUL_PERFORMANCE IN('GOOD','QUEST') AND
    PURPOSE IN('GRID') AND
    CRUISE IN('3SH81', '4MF81', '2DA82', '1CH83', '2PO85', '3MF87', '4MF90', '4MF91', '4MF92', '5MF93', '6MF94', '8MF95', '8MF96', '8MF97',
    '5MF98', '2WE99', '5MF99', '6MF00', '3MF01', '4MF02', '5MF03', '5MF04', '6MF05', '4MF06', '5MF07', '4DY08', '4DY09', '3DY10',
     '2DY11', 'DY13-06', 'DY15-05', 'DY17-05', 'DY19-05','WO21-01','DY23-07')"
  )

hauls.b<-sqlQuery(AFSCconnect,BonHaulQuery) # 4064 records (no WO21-01 yet, at least not yet with PRIMARY_NET==Y). 1/12/23 now  4019 ?!
dim(hauls.b) # 3566 records < 2017 matches saved data file <2017 (EcoDAAT, but with PrimaryNet=Y) from last year. 4128 on 8/7/2023
# 4183 on 9/24/2024 (4106 before DY23-07: lost 22 hauls?)

## Note, sometime summer-winter 2022, results produced from this query changed.  1995 and 1994 affected most (most hauls dropped)
### Some other years gained hauls (2000, 2002, 2004). Others changed by smaller amounts. Kimberly says this is due to "Grid" vs "Planksurv"
### On 1/12/23 Kimberly suggests not selecting for PURPOSE for the time being. Since we later select only hauls with Polygonal Areas this is OK.
### As of 8/7/2023, the PURPOSE codes appear to have been reverted, so can use again to subset.


TuckSampleQuery <-
  paste0(
    "SELECT 
    COMMENTS_SPECIMEN, COMMON_NAME_ICHBASE, CRUISE, GMT_DATE_TIME, HAUL_ID, LARVALCATCHPER1000M3, 
    LARVALCATCHPER10M2,LAT,LON, NET, NUMBER_CAUGHT, POLYGONAL_AREA, PRIMARY_NET, PURPOSE, SPECIES_NAME, STATION_NAME, YEAR 
    FROM ECODAAT_DEV.SPECIMEN_MAIN_GEOM 
    WHERE GEAR_NAME LIKE 'TUCK1' AND
    STAGE_ID=6 AND    
    SPECIES_NAME IN ('Ammodytes personatus','Atheresthes stomias','Bathymaster spp.','Gadus chalcogrammus',
    'Gadus macrocephalus','Hippoglossoides elassodon','Hippoglossus stenolepis','Lepidopsetta bilineata',
    'Lepidopsetta polyxystra','Platichthys stellatus','Sebastes spp.','Stenobrachius leucopsarus') AND
    PRIMARY_NET IN('NPQ') AND
    HAUL_PERFORMANCE IN('GOOD','QUEST') AND
    PURPOSE IN('GRID', 'GRIDPOST') AND
    CRUISE IN('4MF88', '4MF89')"
  )

samples.t<-sqlQuery(AFSCconnect,TuckSampleQuery) # 1532 values, same as saved data file from EcoDAAT.

TuckHaulQuery <-
  paste0(
    "SELECT 
    COMMENTS_HAUL, CRUISE, DAY, GEAR_NAME, GMT_DATE_TIME, HAUL_ID, HAUL_NAME,       
    HAUL_PERFORMANCE, LAT, LON, MAX_GEAR_DEPTH, MESH, MONTH, NET,           
    POLYGONAL_AREA, PRIMARY_NET, PURPOSE,  STATION_NAME, YEAR    
    FROM ECODAAT_DEV.HAUL 
    WHERE GEAR_NAME LIKE 'TUCK1' AND
    PRIMARY_NET IN('NPQ') AND
    HAUL_PERFORMANCE IN('GOOD','QUEST') AND
    PURPOSE IN('GRID','GRIDPOST') AND
    CRUISE IN('4MF88', '4MF89')"
  )

hauls.t<-sqlQuery(AFSCconnect,TuckHaulQuery) # 248 records, matches previous file. 

odbcClose(AFSCconnect)


# Merge 60Bon and Tucker
hauls<-rbind(hauls.b,hauls.t)
samples<-rbind(samples.b,samples.t)

# Save raw data files 
setwd("C:/Users/will.fennie/Work/AFSC Research/OpenScapes/Data")
write.csv(hauls,"Data_For2024OpenScapes_IchTS_HaulRecords_Sep2024.csv",row.names=F)
write.csv(samples,"Data_For2024OpenScapes_IchTS_SpecimenRecords_Sep2024.csv",row.names=F)



#Merge catch data with list of all stations sampled to generate NAs that are then converted to 0s----



# Convert time format using lubridate, and find day of year (YDAY)
hauls$DATE<-ymd_hms(hauls$GMT_DATE_TIME,tz = "UTC")

hauls$DATE[is.na(hauls$DATE)==TRUE] <- ymd(hauls$GMT_DATE_TIME[is.na(hauls$DATE)==TRUE])
# two hauls fail - missing time
hauls$YDAY<-yday(hauls$DATE)

#### CATCH WITH ZEROS ###
#Merge haul data (all quantitative tows) and specimen data (all positive catches) and transform into catch w zero.

## This could all be done more smoothly with DPLYR but keeping for now for consistency.

#Create a dataframe with one row for each speices/haul combo - when merged with the samples df it will generate NAs for 
#stations without larvae of a particular species. These will then be converted to 0s.

specieslist<-c(as.character(unique(samples$SPECIES_NAME)))
haullist<-unique(hauls$HAUL_ID)
tt<-expand.grid(haullist,specieslist) #creates one row for each species/haul
colnames(tt)<-c("HAUL_ID","SPECIES_NAME")
# Merge positive catch data (CPUE) with expanded dataframe, leaving NAs when there is no CPUE value in samples for a given species/haul
all0<-merge(samples[,c("HAUL_ID","SPECIES_NAME","LARVALCATCHPER10M2","LARVALCATCHPER1000M3","NUMBER_CAUGHT")],tt,by=c("HAUL_ID","SPECIES_NAME"),all.y=TRUE)
# Replace the NAs with zeros, since these are true zeros.
all0$LARVALCATCHPER1000M3[is.na(all0$LARVALCATCHPER1000M3)==T]<-0
all0$LARVALCATCHPER10M2[is.na(all0$LARVALCATCHPER10M2)==T]<-0
all0$NUMBER_CAUGHT[is.na(all0$NUMBER_CAUGHT)==T]<-0
# Merge with haul data to get extra data fields, e.g. Lat, Lon, associated with hauls
all0<-merge(all0,hauls,by="HAUL_ID")

#Check relative abundance of species
tapply(all0$LARVALCATCHPER10M2,all0$SPECIES_NAME,mean)

mydat<-all0

plot(mydat$YDAY~mydat$YEAR) ## all are within day 126-159. 
# Historically I used 137-158 which is when the core area was sampled for all cruises except 2019 (and maybe 2021)
# Now that we use all data in the time series calcs, I think I will include the broader dates. 

#write.csv(mydat,"../Data/Data_For2024Update/GOALarvalCatchwZeros_allareas_to2023_d126to159_Sep2024.csv",row.names=F)
