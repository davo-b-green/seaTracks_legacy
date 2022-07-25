### Calculate foraging metrics for dive data

## Author: David Green
## Date: 28-06-2022

library(tidyverse)
library(data.table)
library(nlme)
library(purrr)
## Bring in dive dataset
dive <- fread("./processed_datasets/loc_ssm_dive.csv")

# dive_s <- dive %>% 
#   slice_sample(n = 5000)


## Creating general dive metrics

dive.met <- dive %>% 
  select(id, date, depth_str, propn_str, dive_dur, max_dep) %>% 
  filter(max_dep > 0) %>% 
  rowwise() %>% 
  mutate(
    depth_str = list(as.numeric(str_split(depth_str, ",")[[1]])),
    propn_str = list(as.numeric(str_split(propn_str, ",")[[1]])),
    time_str = list(dive_dur*(propn_str/100)),
    dep_diff_str = list(c(0, depth_str,0)),
    time_diff_str = list(c(0, time_str,dive_dur)),
    dep_diff_str = list(abs(dep_diff_str-lag(dep_diff_str))),
    time_diff_str = list(abs(time_diff_str-lag(time_diff_str))),
    speed_str = list(unlist(list(na.omit(dep_diff_str/time_diff_str)))),
    bot_time = time_str[ # Get time of last recorded depth that is >= 80% of maximum
      detect_index(.x = depth_str/max_dep,
                   .f = ~ . >= 0.8,
                   .dir = "backward")
      ] - time_str[ # Get time of first recorded depth that is >= 80% of maximum
        detect_index(.x = depth_str/max_dep,
                     .f = ~ . >= 0.8)
        ]
    )


dive.met <- dive.met %>% 
  rowwise() %>% 
  mutate(
    hunt = sum(na.omit(time_diff_str)[unlist(speed_str)<0.4])
  )


## Calculating behavioural residuals 

# dive <- fread("./processed_datasets/loc_ssm_dive.csv")
dive.res <- dive %>% 
  select(id, date, dive_dur, max_dep, surf_dur) %>% 
  mutate(log_dur = log(dive_dur),
         log_dep = log(max_dep),
         log_surf = log(surf_dur)
         ) %>% 
  filter(
    # id %in% unique(id)[1:5] &
      is.finite(log_dur) &
      is.finite(log_dep) &
      is.finite(log_surf) 
     )
sum.dive <- dive.res %>% 
  group_by(id) %>% 
  summarise(n = n()) %>% 
  filter(n > 2) %>% 
  pull(id)

fit.dat <- dive.res %>% 
  filter(id %in% sum.dive)

## Calculating dive residual
fit.dr <- lme(log_dur~log_dep, random = ~log_dep|id, data = fit.dat)
fit.dat$dr <- residuals(fit.dr)

## Calculating surface residual

sr.dat <- fit.dat %>% 
  group_by(id, log_dur) %>% 
  summarise(min_sr = min(log_surf)) 

fit.sr <- lme(min_sr~log_dur, random = ~log_dur|id, data = sr.dat)

sr.dat$sr_pred <- exp(fitted(fit.sr))

fit.dat <- left_join(fit.dat, sr.dat, by = c("id", "log_dur")) %>% 
  select(-min_sr) %>% 
  mutate(sr = log(1+(surf_dur-sr_pred)/sr_pred)) %>% 
  select(id, date, dr, sr)

## Bringing together general metrics and behavioural residuals

dive.met <- left_join(dive.met, fit.dat, by = c("id", "date"))

fwrite(dive.met, file = "./processed_datasets/dive-metrics.csv")
