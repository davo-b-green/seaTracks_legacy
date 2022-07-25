##run foieGras on the Weddell ARGOS data
##runs at 2 hour intervals for more accurate interpolation of dive data
##this is the dplyr version
## we use the "rw" rather than "crw" as it produces less spikes and loops
##we use vmax of 0.25 after comparison with othe speeds showed less problems with spikes
##N.B. there are frequent > 4 day gaps in the data which cause gaps in the time series
##which are dealt with subsequnetly in "fix_gaps.r"

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
library(future.apply)

dive <- fread(here("compiled_raw_datasets/dive_all_raw_pre-qc.txt"))  %>% 
  mutate(id = ref,
  date = de_date
  ) 
tdive <- dive %>% 
  select(id, date) %>% 
  na.omit()
diag <- fread(here("compiled_raw_datasets/loc_all_raw_pre-qc.txt"))
dir_create(path = "./processed_datasets/")

## Check for and remove already processed individuals
if(file_exists("./processed_datasets/loc_ssm_dive.csv")){
  camp_comp <- fread("./processed_datasets/loc_ssm_dive.csv") %>% 
    pull(id) %>% 
    unique()
  dive <- dive %>% 
    filter(!(ref %in% camp_comp))
  diag <- diag %>% 
    filter(!(ref %in% camp_comp))
}

# Bring in meta data
idmeta <- fread("IMOS_CTD_metadata.csv")
idmeta <- idmeta %>%
  dplyr::rename(id=SMRU_Ref)

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

lst_date <- d1 %>% 
  group_by(id) %>% 
  summarise(first = min(date),
            last = max(date)
  )
tdive <-  left_join(tdive, lst_date, by = "id") %>% 
  filter(date > first &
           date < last) %>% 
  select(-c(first, last))

tdive <- tdive %>% 
  filter(id %in% dur) %>% 
  arrange(id, date) %>% 
  group_by(id) %>% 
  distinct(date, .keep_all = TRUE) 

## Step 2. Fit the SSM
## uses a speed max of 4 m/s (approx 14.4km/h)
d3 <- data.frame(d1) %>% 
  filter(id %in% unique(tdive$id))
  
## time step of 24 = 1 per day

# Processing datasets in parallel
par.ls <- split(unique(tdive$id), cut(seq_along(unique(tdive$id)), 104, labels = FALSE))

plan("future::multisession")
ts <- Sys.time()
fit_all <- future_lapply(par.ls, function(z) {
  fits <- foieGras::fit_ssm(subset(d3, id %in% z), vmax=4, model="crw", 
                            control = ssm_control(optim=c("nlminb")), 
                            time.step = subset(tdive, id %in% z))
  return(grab(fits, "predicted", as_sf=FALSE))
})
difftime(Sys.time(),ts)


fit_all <- bind_rows(fit_all)
fit_new <- fit_all
# Append to existing processed dataset if available
if(file_exists("./processed_datasets/fit_all_dive.Rdata")){
  # fit_new <- fit_all
  load(file= "./processed_datasets/fit_all_dive.Rdata")
  fit_all <- bind_rows(fit_all, fit_new)
}

##save the output
save(fit_all, file= "./processed_datasets/fit_all_dive.Rdata")

##Step 3. Fit a move persistence model

##keep only the variables needed for the model
dmp <- fit_new %>% dplyr::select(id, date, lon, lat)

##fit the move persistance model
par.ls <- split(unique(dmp$id), cut(seq_along(unique(dmp$id)), 104, labels = FALSE))
plan("future::multisession")
ts <- Sys.time()
mpm_fit <- future_lapply(par.ls, function(z) {
  fmpm <- fit_mpm(subset(dmp, id %in% z), model = "mpm")
  return(grab(fmpm, "fitted", as_sf=FALSE))
})
difftime(Sys.time(),ts)

mpm_fit <- bind_rows(mpm_fit)

##extract the results

##..and merge them with the SSM output
dmp <- data.frame(dmp)
ssm <- left_join(dmp, mpm_fit, by=c("id", "date"))

##..finally add the deployment information
ssm <- left_join(ssm, dive, by=c("id", "date"))
ssm_dive <- left_join(ssm, idmeta, by=c("id"))

# Append to existing processed dataset if available
if(file_exists("./processed_datasets/ssm_dive.Rdata")){
  ssm_dive_new <- ssn_6h
  d1_new <- d1
  load(file = "./processed_datasets/ssm_dive.Rdata")
  ssm_dive <- bind_rows(ssm_dive, ssm_dive_new)
  d1 <- bind_rows(d1, d1_new)
}

save(ssm_dive, d1, file = "./processed_datasets/ssm_dive.Rdata")
fwrite(ssm_dive, file = "./processed_datasets/loc_ssm_dive.csv", sep = ",")
