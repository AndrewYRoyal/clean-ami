library('data.table')
library('magrittr')
library('optparse')
library('aws.s3')
library('yaml')
# setwd('D:\\Users\\aroyal641\\Desktop\\workspace\\MFC-PGE\\clean-ami')
# TODO: make seperate script for reconciling meter counts (maybe to be run locally or on DB)

parser = OptionParser() %>%
  add_option(
    c('-c', '--config'),
    action = 'store',
    type = 'character',
    default = 'config_default.yml',
    help = 'Path to configuration YAML file.') %>%
  add_option(
    c('-d', '--delete'),
    action = 'store_true',
    type = 'logical',
    default = FALSE,
    help = 'Delete existing files in clean directory.'
  )

argsv = parse_args(parser)
cfg = read_yaml(argsv$config)

if(argsv$delete) unlink(cfg$data$clean_dir, recursive = TRUE)
if(!file.exists(cfg$data$clean_dir)) dir.create(cfg$data$clean_dir) 

startTime = Sys.time()

## Import Data
#==================================================
cat('Importing data ... \n')
cfg$col_names = cfg$col_names[unlist(cfg$col_include)]
cfg$col_classes = cfg$col_classes[unlist(cfg$col_include)]
names(cfg$col_classes) = cfg$col_names[names(cfg$col_classes)]

use_dat = fread(
  cfg$data$blocked,
  select = unname(unlist(cfg$col_names)), 
  col.names = setNames(names(cfg$col_names), cfg$col_names), 
  colClasses = unlist(cfg$col_classes),
  key = unlist(cfg$key_cols))

## Select Random Subsample
#==================================================
if(cfg$subsample$sample) { 
  cat(sprintf('Processing random %s meters... \n', format(cfg$subsample$meters, big.mark = ',')))
  ids = unique(use_dat[['sp']])
  set.seed(cfg$seed)
  subset_ids = sample(ids, size = cfg$subsample$meters)
  use_dat = use_dat[sp %in% subset_ids]
}

## Convert to Long Form
#==================================================
cat('Converting to long form... \n')
use_dat = melt(use_dat, id.vars = setdiff(names(use_dat), c('gas', 'elct')), value.name = 'use', variable.name = 'fuel')
setkey(use_dat, sp)
use_dat[channel == 'R', fuel:= 'gen']
use_dat[, c('channel'):= NULL]

## Format Shutdown Days
#==================================================
# if('shutdown' %in% select_cols) {
# }

## Censor Outliers
#==================================================
cat('Detecting outliers... \n')
meter_stat = use_dat[, .(
  avg = mean(use, na.rm = TRUE),
  sd = sd(use, na.rm = TRUE),
  q95 = quantile(use, 0.95, na.rm = TRUE),
  q100 = quantile(use, 1, na.rm = TRUE)),
  by = c('sp', 'fuel')]
meter_stat[, long_tail:= as.numeric(q100 / q95 > 4)]
num_stats = c('avg', 'sd', 'q95', 'long_tail')
meter_stat[, (num_stats):= lapply(.SD, round, 3), .SDcols = num_stats]
meter_stat = meter_stat[, .SD, .SDcols = c('sp', 'fuel', num_stats)]

cat('Censoring outliers... \n')
use_dat = merge(use_dat, meter_stat, by = c('sp', 'fuel'))
stdev_outlier = quote((use - avg) / sd > 3)
quantile_outlier = quote((long_tail == 1) & (use > q95))

use_dat[, outlier:= (eval(stdev_outlier)) | (eval(quantile_outlier))]
use_dat[is.na(outlier), outlier:= FALSE]
use_dat[(outlier) & (fuel != 'gen'), use:= NA]
use_dat[, (num_stats):= NULL]

## Fill Missing Dates
#==================================================
cat('Fill missing dates... \n')
fill_dates = function(dat, date_seq) {
  id_values = unique(dat[['sp']])
  date_grid = expand.grid(id_values, date_seq, stringsAsFactors = FALSE) %>%
    as.data.table
  setnames(date_grid, c('sp', 'date'))
  merge(dat, date_grid, by = c('sp', 'date'), all = TRUE)
}

date_seq = as.IDate(seq.Date(from = min(use_dat$date), to = max(use_dat$date), by = 'day'))

use_dat = split(use_dat, by = 'fuel', keep.by = FALSE) %>%
  lapply(fill_dates, date_seq = date_seq) %>%
  rbindlist(idcol = 'fuel')

use_dat[, msng_date:= is.na(outlier)] # << Missing dates detected by missing non-key column
use_dat[is.na(outlier), outlier:= FALSE]

## Impute missing values
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

meters_n = uniqueN(use_dat[, .SD, .SDcols = 'sp'])
impute_value_n = make_counter(impute_value, n = meters_n)

cat('Imputing missing intervals... \n')
use_dat = split(use_dat[fuel != 'gen'], by = 'sp') %>%
  lapply(impute_value_n, value = 'use') %>%
  rbindlist %>%
  rbind(use_dat[fuel == 'gen'], fill = TRUE) # << Generation is not imputed w/ averages
setnames(use_dat, 'imputed_use', 'imputed')

use_dat[is.na(use) & (fuel == 'gen'), imputed:= TRUE] 
use_dat[(imputed) & (fuel == 'gen'), use:= 0] # << Impute Generation w/ 0 values

use_dat[, all_msng:= all(is.na(use)), by = 'sp']
use_dat[(all_msng), imputed:= TRUE]
use_dat[(all_msng), use:= 0] # << Impute 0 for meter missing all interval data
use_dat[, all_msng:= NULL]

use_dat[is.na(imputed), imputed:= FALSE]

## Reduce Memory Overhead
#==================================================
use_dat[, use:= round(use, 3)]
bool_cols = c('outlier', 'msng_date', 'imputed')
use_dat[, (bool_cols):= lapply(.SD, as.numeric), .SDcols = bool_cols]
# TODO: consider truncating fuel column

## Correct Duplicate Values
#==================================================
#Note: duplication is rare and is typically for generation values
use_dat[, dup:= .N > 1, by = c('sp', 'fuel', 'date')]

use_dat = use_dat[order(sp, fuel, date, imputed)]
use_dat = use_dat[, .SD[1], by = c('sp', 'fuel', 'date')] # << choose non-imputed values

dup_dat = use_dat[(dup), .SD, .SDcols = c('sp', 'fuel')] %>%
  unique %>%
  .[, .(meters = .N), by = 'fuel']

for(f in c('gas', 'elct', 'gen')) {
  sprintf('%s %s meters corrected for dupicate values. \n', dup_dat[fuel == f]$meters, f) %>% cat
}

dup_dat[, dup:= NULL]

## Export
#==================================================
if(cfg$batches$batch) {
  cat('Exporting data in batches... \n')
  row_count = dim(use_dat)[1]
  batch_bins = c(seq(1, row_count, by = cfg$batches$size), Inf)
  use_dat[, batch:= cut(.I, breaks = batch_bins, labels = FALSE, include.lowest = TRUE)]
  batches = 1:max(use_dat$batch)
  for(b in batches) {
    export_path = paste0(cfg$data$clean_dir, sprintf('/batch%s.csv', b))
    col_names = setdiff(names(use_dat), 'batch')
    fwrite(use_dat[batch == b, .SD, .SDcols = col_names], file = export_path, na = 'NA')
  }
} else {
  cat('Exporting data in bulk... \n')
  export_path = paste0(cfg$data$clean_dir, '/bulk.csv')
  fwrite(use_dat, file = export_path, na = 'NA')
}

## Sync to S3
#==================================================
cat('Syncing results to s3... \n')
aws.s3::s3sync(cfg$data$clean_dir, bucket = 'ami-output', direction = 'upload')

time_diff = Sys.time() - startTime
cat(round(time_diff, 1), attr(time_diff, 'units'), 'elapsed. \n')
