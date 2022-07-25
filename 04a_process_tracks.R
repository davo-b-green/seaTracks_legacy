##run foieGras on the ARGOS data
##runs at 6 hour intervals for more accurate interpolation of dive data
##this is the dplyr version
## we use the ""crw" 
##we use vmax of 4 m.s-1
##N.B. there are frequent > 4 day gaps in the data which cause gaps in the time series


library(tidyverse)
library(foieGras)
library(bsam)
library(geosphere)
library(viridis)
library(mapdata)
library(maps)
library(maptools)
library(rnaturalearth)
library(ggspatial)
library(sf)
library(fs)
library(trip)
library(here)
library(data.table)
library(lubridate)
library(future.apply)

# Set timestep
tstep <- 6

diag <- fread(here("compiled_raw_datasets/loc_all_raw_pre-qc.txt"))
dir_create(path = "./processed_datasets/")

## Check for and remove already processed individuals
if(file_exists("./processed_datasets/loc_ssm_6h.csv")){
  camp_comp <- fread("./processed_datasets/loc_ssm_6h.csv") %>% 
    pull(id) %>% 
    unique()
  diag <- diag %>% 
    filter(!(ref %in% camp_comp))
}

# Bring in meta data
idmeta <- fread("IMOS_CTD_metadata.csv")
idmeta <- idmeta %>%
  dplyr::rename(id=ref) 

st_date <- idmeta %>%  
  mutate(dep_date = as.Date(paste(Year, Month, Day, sep = "-"), tz = "UTC")) %>% 
  select(id, dep_date)

rr1 <-  diag %>% 
  dplyr::select(ref, d_date, lq, lon, lat, campaign) %>% 
  dplyr::rename(id=ref, lc=lq, date = d_date) %>% 
  mutate(lc = case_when(lc == -9 ~ "Z", ## convert lc to foieGras format
                        lc == -2 ~ "B",
                        lc == -1 ~ "A",
                        TRUE ~ as.character(lc) # Remaining values stay the same
                        )
         )
rr1 <- left_join(rr1, st_date, by = "id") %>% 
  filter(date > dep_date) %>% 
  select(-dep_date)
  

############################################################
## only keep seals with 5 days of data
dur <- rr1 %>% 
  group_by(id) %>% 
  summarise(first=min(date), 
            last=max(date)
            ) %>% 
  mutate(dur = difftime(last, first)
         ) %>%
  filter(dur > 120) %>%
  pull(id)

## ensure the data are sorted by id and date and remove duplicates and locations too far north
d1 <- rr1 %>% 
  filter(id %in% dur) %>% 
  arrange(campaign, id, date) %>% 
  group_by(id) %>% 
  distinct(date, .keep_all = TRUE) %>%
  dplyr::select(1:5)

d1_new <- d1
## Find all time gaps of 4 or more days
tgaps <- d1 %>% 
  group_by(id) %>% 
  mutate(tgap = difftime(time1 = lead(date),
                         time2 = date,
                         units = "days"
                        ),
         n.date = lead(date)) %>% 
  filter(tgap > 4) %>% 
  dplyr::select(c(id, date, n.date))

## Step 2. Fit the SSM
## uses a speed max of 4 m/s (approx 14.4km/h)
d3 <- data.frame(d1) 

par.ls <- split(unique(d3$id), cut(seq_along(unique(d3$id)), 104, labels = FALSE))

plan("future::multisession")
ts <- Sys.time()
fit_all <- future_lapply(par.ls, function(z) {
  ## time step of 24 = 1 per day
  fits <- foieGras::fit_ssm(subset(d3, id %in% z), vmax=4, model="crw", 
                            control = ssm_control(optim=c("nlminb")), 
                            time.step = tstep)
  return(grab(fits, "predicted", as_sf=FALSE))
})
difftime(Sys.time(),ts)

fit_all <- bind_rows(fit_all)
fit_new <- fit_all
## Append fits to existing processed dataset (if it exists)
if(file_exists("./processed_datasets/fit_all_6h.Rdata")){
  # fit_new <- fit_all
  load(file= "./processed_datasets/fit_all_6h.Rdata")
  fit_all <- bind_rows(fit_all, fit_new)
}

##save the output
save(fit_all, file= "./processed_datasets/fit_all_6h.Rdata")

##Step 3. Filter out points associated with land, and where gaps of >4 days exist between satellite fixes
dmp <- fit_new

##keep only the variables needed for the model
dmp1 <- dmp %>% dplyr::select(id, date, lon, lat)

##Cut all locations within 5km of start of trip

sf_use_s2(FALSE)

ne_buffer <- st_read("./land_buffer/land_buffer.shp")

par.ls <- split(unique(dmp1$id), cut(seq_along(unique(dmp1$id)), 104, labels = FALSE))

sfp <- st_as_sf(x = dmp1, 
                coords = c("lon", "lat"), 
                crs = st_crs(ne_buffer))

plan("future::multisession")

ts <- Sys.time()
sfp <- future_lapply(par.ls, function(z) {
  sfp_sub = subset(sfp, id %in% z)
  sfp_sub = st_join(
    sfp_sub,
    ne_buffer,
    join = st_within
  ) %>% 
    data.frame() %>% 
    mutate(home = case_when(
      type %in% "land" ~ 1,
      TRUE ~ 0
    ))
    sfp_sub = data.frame(sfp_sub) %>% 
    dplyr::select(-c(geometry, type)) 
  sfp_sub$home <- as.integer(sfp_sub$home)
  return(sfp_sub)
})
sfp <- bind_rows(sfp)
difftime(Sys.time(),ts)

dmp1 <- left_join(dmp1, sfp, by = c("id", "date"))


# write_csv(dmp1, file = "./processed_datasets/track-overlap-land.csv") # can save output if necessary

## If locations extend north of 30S for more than 1 day, then retrospectively remove that section up to the last location associated with land
# dmp1 <- read_csv(file = "./processed_datasets/track-overlap-land.csv")

dmp1 <- dmp1 %>% 
  group_by(id) %>% 
  mutate(
    north = ifelse(lat > -30, 1, 0),
    home = ifelse(lat > -30, 0, home),
    north = frollapply(north, 
                      n = floor(24/tstep),
                      align = "left",
                      
                      FUN = function(x){
                        all(x == 1)
                      })
  ) %>% 
  tidyr::fill(north, .direction = "down") #%>% 

dmp1 <- lapply(split(dmp1, f = dmp1$id), function(x){
            # print(x$id[1])
            ii = nrow(x)
            if(x$north[ii]==1){
            while(x$home[ii]==0){
              x$north[ii] = 1
              ii = ii-1
              if(ii==0)break
            }}
           return(x) 
          }) %>% 
          bind_rows() %>% 
          filter(north<1)

## Remove track sections associated with the beginning and ends of trips
dmp1 <- dmp1 %>%  
  group_by(id) %>%  
  mutate(
    away = frollapply(home, 
                      n = floor(24/tstep),
                      align = "left",
                      
                      FUN = function(x){
                        all(x < 1)
                      }),
    notback = frollapply(rev(home), 
                         n = floor(24/tstep),
                         align = "left",
                         
                         FUN = function(x){
                           all(x < 1)
                         }),
    notback = rev(notback),
    away = case_when(is.na(away) ~ notback,
                     TRUE ~ away),
    notback = case_when(is.na(notback) ~ away,
                        TRUE ~ notback),
    away = cumsum(away),
    notback = rev(cumsum(rev(notback)))
  ) %>% 
  filter(away > 0 & notback > 0) %>% 
  dplyr::select(id, date, lon, lat)

############################################################
## Once again only keep seals with 5 days of data
dur <- dmp1 %>% 
  group_by(id) %>% 
  summarise(first=min(date), 
            last=max(date)
  ) %>% 
  mutate(dur = difftime(last, first)
  ) %>%
  filter(dur > 5) %>%
  pull(id)

dmp1 <- dmp1 %>% 
  filter(id %in% dur)

## Also remove all sections where there is a gap of more than 4 days between fixes

dmp1 <-  unique(tgaps$id) %>% 
  map_dfr(function(x){
    subdat = subset(dmp1, id %in% x) %>% 
      mutate(tgap = date %within% interval(subset(tgaps, id %in% x)$date,
                                           subset(tgaps, id %in% x)$n.date)
             ) 
    return(subdat)
    }) %>% 
  bind_rows() %>% 
  full_join(dmp1, by = c("id", "date", "lon", "lat")) %>% 
  complete(fill = list(tgap = 0)) %>% 
  filter(tgap == 0) %>% 
  dplyr::select(-tgap)

  
##Step 4. Fit a move persistence model

##fit the move persistance model

par.ls <- split(unique(dmp1$id), cut(seq_along(unique(dmp1$id)), 104, labels = FALSE))
plan("future::multisession")
ts <- Sys.time()
mpm_fit <- future_lapply(par.ls, function(z) {
  fmpm <- fit_mpm(subset(dmp1, id %in% z), model = "mpm")
  return(grab(fmpm, "fitted", as_sf=FALSE))
})
difftime(Sys.time(),ts)

mpm_fit <- bind_rows(mpm_fit)
plan(sequential)

##..and merge them with the SSM output
dmp <- data.frame(dmp1)
ssm <- left_join(dmp1, mpm_fit, by=c("id", "date"))

##..finally add the deployment information

ssm_6h <- left_join(ssm, idmeta, by="id")
ssm_6h_new <- ssm_6h
d1_new <- d1
## Append current tracks to previously processed track dataset (if it exists)
if(file_exists("./processed_datasets/fit_all_6h.Rdata")){
  # ssm_6h_new <- ssm_6h
  # d1_new <- d1
  load(file = "./processed_datasets/ssm_6h.Rdata")
  ssm_6h <- bind_rows(ssm_6h, ssm_6h_new)
  d1 <- bind_rows(d1, d1_new)
}

save(ssm_6h, d1, file = "./processed_datasets/ssm_6h.Rdata")
fwrite(ssm_6h, file = "./processed_datasets/loc_ssm_6h.csv", sep = ",")
