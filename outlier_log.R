library('data.table')
library('magrittr')
library('optparse')
library('aws.s3')

# if(interactive()) {
#   argsv = list(utility = 'electric', `address-only` = FALSE)
# }

# parser = OptionParser() %>%
#   add_option()
# argsv = parse_args(parser)

id_cols = c('service_point_id')

import_paths = list(
  consumption = 'input/daily.csv',
  meter_site = 'input/meter_site.csv')

export_paths = list(
  outliers = 'output/outliers.csv')

cat('Getting Meter-Site from s3 ... \n')
if(!file.exists('input')) dir.create('input')
aws.s3::s3sync('input', bucket = 'ami-input', direction = 'download')

startTime = Sys.time()

## Import Data
#==================================================
cat('Importing data ... \n')
col_names = c('interval_date' = 'date', 'therms' = 'use')

use_dat = fread(import_paths$consumption, select = c(id_cols, names(col_names)))
use_dat[, (id_cols):= lapply(.SD, as.character), .SDcols = id_cols]
setnames(use_dat, names(col_names), col_names)
setkeyv(use_dat, id_cols)

meter_site = fread(import_paths$meter_site, key = 'site') %>%
  .[grepl('[cr]', site)] 
meter_site = meter_site[type == 'gas']

## Format Columns
#==================================================
cat('Formatting columns ... \n')
meter_site[, (id_cols):= gsub('m', '', meterID)]
meter_site = meter_site[, .SD, .SDcols = c('site', id_cols)]
setkeyv(meter_site, id_cols)

use_dat[, use:= as.double(use)]

## Calculate Metrics
#==================================================
use_dat = merge(use_dat, meter_site, by = id_cols)
use_dat[, site:= NULL]

outlier_dat = use_dat[, .(
  avg = mean(use, na.rm = TRUE),
  median = median(use, na.rm = TRUE),
  max = max(use, na.rm = TRUE),
  sd = sd(use, na.rm = TRUE), 
  n = sum(!is.na(use))),
  by = id_cols]

z_exceed = merge(use_dat, outlier_dat[, .SD, .SDcols = c(id_cols, 'avg', 'sd')]) %>%
  .[, .(
    z_5 = mean((use - avg) / sd > 5, na.rm = TRUE),
    z_3 = mean((use - avg) / sd > 3, na.rm = TRUE)), 
    by = id_cols]
outlier_dat = merge(outlier_dat, z_exceed) 

## Export
#==================================================
if(!dir.exists('output')) dir.create('output')

fwrite(outlier_dat, export_paths$outliers, na = 'NA')

time_diff = Sys.time() - startTime
cat(round(time_diff, 1), attr(time_diff, 'units'), 'elapsed. \n')

cat('Syncing results to s3... \n')
aws.s3::s3sync('output', bucket = 'ami-output', direction = 'upload')
