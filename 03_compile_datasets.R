# Title: Code for extracting location and dive data from associated ACCESS databases, and compiling 
# into master location and dive datasets
# Compiled by David Green
# Date: 17-05-2022
# Edited: 15-06-2022

# ----------------------------------------------------------------------------------------------------------------------------------- #
library(curl)
library(RODBC)
library(tidyverse)
library(fs)
library(rSRDL)
library(here) # sets root directory
library(future.apply)
library(data.table)
# ----------------------------------------------------------------------------------------------------------------------------------- #

dir_create(path = here("compiled_raw_datasets")) # Create directory for the compiled loc, dive, ctd and haulout datasets

# ----------------------------------------------------------------------------------------------------------------------------------- #
# Combining datasets #

## Get list of all ACCESS files
fl <- dir_ls(path = here("access_files"),
             glob = "*.mdb")

## Check for already processed campaigns
if(file_exists("./compiled_raw_datasets/loc_all_raw_pre-qc.txt")){
  camp_comp <- fread("./compiled_raw_datasets/loc_all_raw_pre-qc.txt") %>% 
    pull(campaign) %>% 
    unique()
  p <- str_split(fl[1], "/")[[1]]
  p <- paste0(p[1:length(p)-1], collapse = "/")
  camp_comp <- paste0(p,"/",camp_comp,".mdb")
}else{
  camp_comp <- NULL
}

# Get only campaigns that haven't yet been compiled
fl <- subset(fl,
            !(fl %in% camp_comp))

# ----------------------------------------------------------------------------------------------------------------------------------- #
## Combine location data files

if(length(fl) == 0){par.ls <- NULL; cat("No new location data available")}else{
par.ls <- split(fl, cut(seq_along(fl), floor(length(fl)/5), labels = FALSE))
}
plan("future::multisession")

dataLoc <- future_lapply(par.ls, function(z){
  subDat <- lapply(z, function(x){
  fl_split = str_split(x, pattern = "/")[[1]]
  theDB = fl_split[length(fl_split)] %>% 
    str_remove(pattern = ".mdb")
  thePath = paste0(fl_split[1:(length(fl_split)-1)], 
                   collapse = "/")
  cat("Processing locations: ", theDB, "\n")
  con = odbcDriverConnect(paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=",x))
  data = sqlFetch(con,
                  "diag",
                  as.is = TRUE
                  )
  data = select(data, -which(as.character(sapply(data, class)) %in% "ODBC_binary")) %>%  # remove unrecognized ODBC_binary variables
     mutate(
      across( # Correcting decimal degrees that have comma separator
        where(~ is.character(.x) &&
                isTRUE(
                  max(str_count(.x, ","), na.rm = TRUE) == 1 &&
                    any(str_count(.x, ",")) == 1
                  )
          ),
        ~ as.numeric(str_replace(.x, ",", "."))
      ),
      across( # Coercing everything to character prior to binding
        .cols = everything(),
        .fns = as.character
        )
    ) %>% 
    rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase

  odbcClose(con) # close ACCESS db connection
  return(data)

  iris %>% 
    rename_with(~ stringr::str_replace(.x, 
                                       pattern = "Length", 
                                       replacement = "len"), 
                matches("Length"))   
  
  
}) %>% 
  bind_rows() %>% 
  rowwise() %>% 
  mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
         d_date = as.POSIXct(d_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"))
}, future.seed = TRUE) %>% 
  bind_rows()
plan(sequential)

# Read in previously processed dataset if it exists
if(file_exists("./compiled_raw_datasets/loc_all_raw_pre-qc.txt")){
  olderLoc <- fread("./compiled_raw_datasets/loc_all_raw_pre-qc.txt") 
}else{
  olderLoc <- data.frame()
}

dataLoc <- bind_rows(olderLoc, dataLoc) # append newly compiled data onto older dataset

# Save dataset
write_delim(x = dataLoc,
            file = here("compiled_raw_datasets", "loc_all_raw_pre-qc.txt"),
            delim = "\t"
            )

# ----------------------------------------------------------------------------------------------------------------------------------- #
## Create dive dataset

## Get list of all ACCESS files
fl <- dir_ls(path = here("access_files"),
             glob = "*.mdb")

## Check for already processed campaigns
if(file_exists("./compiled_raw_datasets/dive_all_raw_pre-qc.txt")){
  camp_comp <- fread("./compiled_raw_datasets/dive_all_raw_pre-qc.txt") %>% 
    pull(campaign) %>% 
    unique()
  p <- str_split(fl[1], "/")[[1]]
  p <- paste0(p[1:length(p)-1], collapse = "/")
  camp_comp <- paste0(p,"/",camp_comp,".mdb")
}else{
  camp_comp <- NULL
}

# Get only campaigns that haven't yet been compiled
fl <- subset(fl,
             !(fl %in% camp_comp))

if(length(fl) == 0){par.ls <- NULL; cat("No new deployments available")}else{
  par.ls <- split(fl, cut(seq_along(fl), floor(length(fl)/5), labels = FALSE))
}


plan("future::multisession") # Compile dive datasets in parallel
dataDive <- future_lapply(par.ls, function(z){
  subDat <- lapply(z, function(x){
  fl_split = str_split(x, pattern = "/")[[1]]
  theDB = fl_split[length(fl_split)] %>% 
    str_remove(pattern = ".mdb")
  thePath = paste0(fl_split[1:(length(fl_split)-1)], 
                   collapse = "/")
  cat("Processing dives: ", theDB, "\n")

  data = tryCatch( # Catch any errors coming from there being no dive data
    get.SRDLdb(theDB = theDB,
               theTable = "dive",
               theFields = "All",
               theRef = "All",
               thePath = thePath
    ),
    error = function(e){ 
      message(paste("Unable to find dive data for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    })
  
if(is.null(data)){
  # odbcClose(con) # close ACCESS db connection
  return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
}else{  
  data = data %>% 
    qcDive(flag.only = TRUE,
           max.v.speed = 5) %>% 
    mutate(
      across( # Coercing everything to character prior to binding
        .cols = everything(),
        .fns = as.character
      )
    ) %>% 
    rename_with(~ str_replace_all(.x, pattern = "\\.", replacement = "_")) %>% 
    rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase
  
  return(data)
  
}
}) %>%  
  bind_rows() %>% 
  rowwise() %>% 
  mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
         de_date = as.POSIXct(de_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
         )
  return(subDat)
}, future.seed = TRUE) %>% 
  bind_rows()
plan(sequential)

# Read in previously processed dataset if it exists
if(file_exists("./compiled_raw_datasets/dive_all_raw_pre-qc.txt")){
  olderDive <- fread("./compiled_raw_datasets/dive_all_raw_pre-qc.txt") 
}else{
  olderDive <- data.frame()
}

dataLoc <- bind_rows(olderDive, dataDive) # append newly compiled data onto older dataset

# Save dataset
write_delim(x = dataDive,
            file = here("compiled_raw_datasets", "dive_all_raw_pre-qc.txt"),
            delim = "\t"
)
# ----------------------------------------------------------------------------------------------------------------------------------- #

## Create ctd dataset

## Get list of all ACCESS files
fl <- dir_ls(path = here("access_files"),
             glob = "*.mdb")

## Check for already processed campaigns
if(file_exists("./compiled_raw_datasets/ctd_all_raw_pre-qc.txt")){
  camp_comp <- fread("./compiled_raw_datasets/ctd_all_raw_pre-qc.txt") %>% 
    pull(campaign) %>% 
    unique()
  p <- str_split(fl[1], "/")[[1]]
  p <- paste0(p[1:length(p)-1], collapse = "/")
  camp_comp <- paste0(p,"/",camp_comp,".mdb")
}else{
  camp_comp <- NULL
}

# Get only campaigns that haven't yet been compiled
fl <- subset(fl,
             !(fl %in% camp_comp))

# if(length(fl) == 0){par.ls <- NULL; cat("No new deployments available")}else{
#   par.ls <- split(fl, cut(seq_along(fl), floor(length(fl)/5), labels = FALSE))
# }
par.ls = fl

plan("future::multisession") # Compile ctd datasets in parallel
dataDive <- future_lapply(par.ls, function(z){
  subDat <- lapply(z, function(x){
    fl_split = str_split(x, pattern = "/")[[1]]
    theDB = fl_split[length(fl_split)] %>% 
      str_remove(pattern = ".mdb")
    thePath = paste0(fl_split[1:(length(fl_split)-1)], 
                     collapse = "/")
    cat("Processing ctd profiles: ", theDB, "\n")
    
    data = tryCatch( # Catch any errors coming from there being no dive data
      get.SRDLdb(theDB = theDB,
                 theTable = "ctd",
                 theFields = "All",
                 theRef = "All",
                 thePath = thePath
      ),
      error = function(e){ 
        message(paste("Unable to find ctd data for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
      })
    
    if(is.null(data)){
      # odbcClose(con) # close ACCESS db connection
      return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    }else{  
      data = data %>% 
        mutate(
          across( # Coercing everything to character prior to binding
            .cols = everything(),
            .fns = as.character
          )
        ) %>% 
        rename_with(~ str_replace_all(.x, pattern = "\\.", replacement = "_")) %>% 
        rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase
      
      return(data)
      
    }
  }) %>%  
    bind_rows() %>% 
    rowwise() %>% 
    mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
           end_date = as.POSIXct(end_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
    )
  return(subDat)
}, future.seed = TRUE) %>% 
  bind_rows()
plan(sequential)

# Read in previously processed dataset if it exists
if(file_exists("./compiled_raw_datasets/ctd_all_raw_pre-qc.txt")){
  olderDive <- fread("./compiled_raw_datasets/ctd_all_raw_pre-qc.txt") 
}else{
  olderDive <- data.frame()
}

dataLoc <- bind_rows(olderDive, dataDive) # append newly compiled data onto older dataset

# Save dataset
write_delim(x = dataDive,
            file = here("compiled_raw_datasets", "ctd_all_raw_pre-qc.txt"),
            delim = "\t"
)
# ----------------------------------------------------------------------------------------------------------------------------------- #

## Create haulout dataset

## Get list of all ACCESS files
fl <- dir_ls(path = here("access_files"),
             glob = "*.mdb")

## Check for already processed campaigns
if(file_exists("./compiled_raw_datasets/haulout_all_raw_pre-qc.txt")){
  camp_comp <- fread("./compiled_raw_datasets/haulout_all_raw_pre-qc.txt") %>% 
    pull(campaign) %>% 
    unique()
  p <- str_split(fl[1], "/")[[1]]
  p <- paste0(p[1:length(p)-1], collapse = "/")
  camp_comp <- paste0(p,"/",camp_comp,".mdb")
}else{
  camp_comp <- NULL
}

# Get only campaigns that haven't yet been compiled
fl <- subset(fl,
             !(fl %in% camp_comp))

if(length(fl) == 0){par.ls <- NULL; cat("No new deployments available")}else{
  par.ls <- split(fl, cut(seq_along(fl), floor(length(fl)/5), labels = FALSE))
}


plan("future::multisession") # Compile haulout datasets in parallel
dataDive <- future_lapply(par.ls, function(z){
  subDat <- lapply(z, function(x){
    fl_split = str_split(x, pattern = "/")[[1]]
    theDB = fl_split[length(fl_split)] %>% 
      str_remove(pattern = ".mdb")
    thePath = paste0(fl_split[1:(length(fl_split)-1)], 
                     collapse = "/")
    cat("Processing haulouts: ", theDB, "\n")
    
    data = tryCatch( # Catch any errors coming from there being no dive data
      get.SRDLdb(theDB = theDB,
                 theTable = "haulout",
                 theFields = "All",
                 theRef = "All",
                 thePath = thePath
      ),
      error = function(e){ 
        message(paste("Unable to find haulout data for", str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
      })
    
    if(is.null(data)){
      # odbcClose(con) # close ACCESS db connection
      return(data.frame(REF = str_split(x, "/")[[1]][length(str_split(x, "/")[[1]])]))
    }else{  
      data = data %>% 
       mutate(
          across( # Coercing everything to character prior to binding
            .cols = everything(),
            .fns = as.character
          )
        ) %>% 
        rename_with(~ str_replace_all(.x, pattern = "\\.", replacement = "_")) %>% 
        rename_with(.cols = everything(), .fn = tolower) # Change all colnames to lowercase
      
      return(data)
      
    }
  }) %>%  
    bind_rows() %>% 
    rowwise() %>% 
    mutate(campaign = str_split(ref, pattern = "[\\_-]+")[[1]][1],
           s_date = as.POSIXct(s_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC"),
           e_date = as.POSIXct(e_date, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
    )
  return(subDat)
}, future.seed = TRUE) %>% 
  bind_rows()
plan(sequential)

# Read in previously processed dataset if it exists
if(file_exists("./compiled_raw_datasets/haulout_all_raw_pre-qc.txt")){
  olderDive <- fread("./compiled_raw_datasets/haulout_all_raw_pre-qc.txt") 
}else{
  olderDive <- data.frame()
}

dataLoc <- bind_rows(olderDive, dataDive) # append newly compiled data onto older dataset

# Save dataset
write_delim(x = dataDive,
            file = here("compiled_raw_datasets", "haulout_all_raw_pre-qc.txt"),
            delim = "\t"
)
# ----------------------------------------------------------------------------------------------------------------------------------- #