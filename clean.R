library('data.table')
library('magrittr')
library('optparse')
library('aws.s3')

# if(interactive()) {
#   argsv = list(utility = 'electric', `address-only` = FALSE)
# }

parser = OptionParser() %>%
  add_option(c('-u', '--utility'),
             action = 'store',
             type = 'character',
             default = 'electric',
             help = 'Utility: gas or electric.') %>%
  add_option(c('-a', '--address-only'),
             type = 'logical',
             default = FALSE,
             help = 'Option to use only address-matched meters.') %>%
  add_option(c('-f', '--filter'),
             action = 'store',
             type = 'character',
             default = 'none',
             help = 'Boolean column used to filter meters.')

argsv = parse_args(parser)

electric = argsv$utility == 'electric'
id_cols = c('service_point_id')

import_paths = list(
  consumption = 'input/daily.csv',
  meter_site = 'input/meter_site.csv')

export_paths = list(
  imputed = 'output/imputed_%s.csv',
  use = 'output/%s.csv',
  has_interval = 'output/has_%s.csv',
  shutdowns = 'output/shutdowns_%s.csv')

cat('Getting Meter-Site from s3 ... \n')
if(!file.exists('input')) dir.create('input')
aws.s3::s3sync('input', bucket = 'ami-input', direction = 'download')

startTime = Sys.time()


## Import Data
#==================================================
cat('Importing data ... \n')
col_names = c(
  'interval_date' = 'date', 
  'shutdown_indicator' = 'shutdown')

if(electric) {
  col_names = c(col_names, c('channel_id' = 'channel', 'kwh' = 'use'))
} else {
  col_names = c(col_names, c('therms' = 'use'))
}

use_dat = fread(import_paths$consumption)
use_dat[, (id_cols):= lapply(.SD, as.character), .SDcols = id_cols]
setnames(use_dat, names(col_names), col_names)
setkeyv(use_dat, id_cols)

meter_site = fread(import_paths$meter_site, key = 'site') %>%
  .[grepl('[cr]', site)] 
meter_site = meter_site[type == argsv$utility]
if(argsv$`address-only`) meter_site = meter_site[method == 'Address']
if(argsv$filter != 'none') meter_site = meter_site[get(argsv$filter)]

## Format Columns
#==================================================
cat('Formatting columns ... \n')
meter_site[, (id_cols):= gsub('m', '', meterID)]
meter_site = meter_site[, .SD, .SDcols = c('site', id_cols)]
setkeyv(meter_site, id_cols)

## Filter meters
#==================================================
cat('Filtering... \n')
has_interval = unique(use_dat[, .SD, .SDcols = id_cols]) %>%
  .[, interval:= TRUE] %>%
  merge(meter_site, all.y = TRUE) %>%
  .[, interval:= !is.na(interval)]

meters_n = uniqueN(meter_site[, .SD, .SDcols = id_cols])
cat(format(meters_n, big.mark = ','), 'meters in set. \n')

use_dat = merge(use_dat, meter_site, by = id_cols)
use_dat[, site:= NULL]

meters_found_n = uniqueN(use_dat[, .SD, .SDcols = id_cols])
cat(format(meters_found_n, big.mark = ','), 'meters with interval data. \n')

## Count Shutdown Days
#==================================================
shutdowns = use_dat[, .SD, .SDcols = c(id_cols, 'date', 'shutdown')]
shutdowns = use_dat[, shutdown:= (shutdown == 'Y')]
shutdowns[is.na(shutdown), shutdown:= FALSE]

shutdowns_n = sum(shutdowns$shutdown)
cat(format(shutdowns_n, big.mark = ','), 'total shutdown meter-days \n')

shutdowns = merge(shutdowns, meter_site, by = id_cols)
shutdowns = shutdowns[, .(shutdown = mean(shutdown)), by = .(site, date)]

## Format Consumption Data
#==================================================
cat('Formatting consumption data... \n')
# use_dat[, date:= as.POSIXct(date, format = '%Y-%m-%d', tz = 'UTC')]
if(electric) {
  gen_dat = use_dat[(channel == 'R'), .SD, .SDcols = c(id_cols, 'date', 'use')]
  setnames(gen_dat, 'use', 'gen')
  gen_dat[, gen:= abs(gen)]
  use_dat = use_dat[!(channel == 'R')]
  use_dat = merge(use_dat, gen_dat, by = c(id_cols, 'date'), all = TRUE)
  rm('gen_dat')
  use_dat[, channel:= NULL]
  use_dat[is.na(gen), gen:= 0]
}
use_cols = intersect(names(use_dat), c(id_cols, 'date', 'use', 'gen'))
use_dat = use_dat[, .SD, .SDcols = use_cols]

## Censor 3-Sigma Outliers
#==================================================
meter_stat = use_dat[, .(
  avg = mean(use, na.rm = TRUE),
  sd = sd(use, na.rm = TRUE),
  q95 = quantile(use, 0.95, na.rm = TRUE),
  q100 = quantile(use, 1, na.rm = TRUE)),
  by = id_cols]

use_dat = merge(use_dat, meter_stat)
use_dat[(use - avg) / sd > 3, use:= NA]
use_dat[(q100 / q95 > 4) & (use > q95), use:= NA]

use_dat[, c('avg', 'sd', 'q95', 'q100'):= NULL]

## Clean at Meter Level
#==================================================
make_counter = function(f, n) {
  x = 0
  step_size = n %/% 20
  fun_name = deparse(substitute(f))
  function(...) {
    if(x == 0) cat(fun_name, '\n', 'Progress:')
    x <<- x + 1
    if(x %% step_size == 0) cat('X')
    if(x == n) cat('100% \n')
    f(...)
  }
}
fill_dates = function(x, date_range, interval = c('hour', 'day')) {
  interval = match.arg(interval)
  id_cols = x[, .SD[1], .SDcols = setdiff(names(x), c('date', 'use', 'gen'))]
  dates = data.table(date = as.IDate(seq.Date(from = min(date_range), to = max(date_range), by = interval)))
  dates = cbind(dates, id_cols)
  merge(x, dates, by = names(dates), all = TRUE)
}
impute_value = function(x, value) {
  indicator_col = paste0('imputed_', value)
  if(!(value %in% names(x))) return(x)
  x[, (indicator_col):= is.na(get(value))]
  if(!anyNA(x[[value]])) return(x)
  x[, imputed:= mean(get(value), na.rm = TRUE), by = .(month(date))]
  x[is.na(get(value)), (value):= imputed]
  x[, imputed:= NULL]
  if(!anyNA(x[[value]])) return(x)
  x[, imputed:= mean(get(value), na.rm = TRUE)]
  x[is.na(get(value)), (value):= imputed]
  x[, imputed:= NULL]
  x
}

meters_n = uniqueN(use_dat[, .SD, .SDcols = id_cols])
fill_dates_n = make_counter(fill_dates, n = meters_n)
impute_value_n = make_counter(impute_value, n = meters_n)

cat('Imputing missing meter data... \n')
use_dat = split(use_dat, by = id_cols) %>%
  lapply(fill_dates_n, date_range = c(min(use_dat$date), max(use_dat$date)), interval = 'day') %>%
  lapply(impute_value_n, value = 'use') %>%
  lapply(impute_value_n, value = 'gen') %>%
  rbindlist
imputed_cols = intersect(names(use_dat), c('imputed_use', 'imputed_gen'))
imputed_meter = use_dat[, lapply(.SD, mean), .SDcols = imputed_cols, by = id_cols]

## Aggregate to Site
#==================================================
cat('\n Aggregating to Site... \n')
use_dat = merge(use_dat, meter_site, by = id_cols)
if(electric) {
  value_agg = quote(.(use = sum(use),
                      imputed_use = weighted.mean(imputed_use, use),
                      gen = sum(gen)))
} else {
  value_agg = quote(.(use = sum(use), imputed_use = weighted.mean(imputed_use, use)))
}
use_dat = use_dat[, eval(value_agg), by = .(site, date)]

## Export
#==================================================
if(!dir.exists('output')) dir.create('output')

f_suffix = argsv$utility
if(argsv$filter != 'none') f_suffix = sprintf('%s_%s', f_suffix, argsv$filter)
export_paths = lapply(export_paths, sprintf, f_suffix)

fwrite(imputed_meter, export_paths$imputed, na = 'NA')
fwrite(use_dat, export_paths$use, na = 'NA')
fwrite(has_interval, export_paths$has_interval, na = 'NA')
fwrite(shutdowns, export_paths$shutdowns, na = 'NA')

time_diff = Sys.time() - startTime
cat(round(time_diff, 1), attr(time_diff, 'units'), 'elapsed. \n')

cat('Syncing results to s3... \n')
if(!file.exists('input')) dir.create('input')
aws.s3::s3sync('output', bucket = 'ami-output', direction = 'upload')
