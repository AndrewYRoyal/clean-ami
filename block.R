library('data.table')
library('magrittr')
library('optparse')
library('aws.s3')
library('yaml')
# setwd('D:\\Users\\aroyal641\\Desktop\\workspace\\MFC-PGE\\clean-ami')

parser = OptionParser() %>%
  add_option(
    c('-c', '--config'),
    action = 'store',
    type = 'character',
    default = 'config_default.yml',
    help = 'Path to configuration YAML file.') %>%
  add_option(
    c('-b', '--blocks'),
    action = 'store',
    type = 'numeric',
    default = 2,
    help = 'Number of blocks to divide the data.'
  ) %>%
  add_option(
    c('-d', '--delete'),
    action = 'store_true',
    type = 'logical',
    default = FALSE,
    help = 'Delete existing files in blocked directory.'
  )
  
argsv = parse_args(parser)
cfg = read_yaml(argsv$config)

if(argsv$delete) unlink('data/blocked', recursive = TRUE)
if(!dir.exists('data/blocked')) dir.create('data/blocked')
if(!dir.exists('log')) dir.create('log')

# Check Column Names and Classes
#======================================================
raw_files = setNames(
  list.files(cfg$data$raw_dir, full.names = TRUE),
  list.files(cfg$data$raw_dir))
trunc_tables = lapply(raw_files, fread, nrows = 1)

lapply(trunc_tables, function(x) x[, lapply(.SD, class)]) %>%
  rbindlist(idcol = 'file', fill = TRUE) %>%
  fwrite('log/classes.csv')

headers = lapply(trunc_tables, names)
headers_equal = Reduce(all.equal, headers)
#if(headers_equal != TRUE) stop('Headers do not match. Check classes log.')

# Collect Service Point IDs
#======================================================
cat('Identified service points... \n')
sp_name = cfg$col_names$sp
extract_service_points = function(file_path) {
  fread(file_path, select = sp_name, colClasses = cfg$col_classes$sp, key = sp_name) %>% unique
}

service_points = lapply(raw_files, extract_service_points) %>%
  rbindlist
setkeyv(service_points, cfg$col_names$sp)
service_points = unique(service_points)

row_count = dim(service_points)[1]
sprintf('%s service points found. \n', format(row_count, big.mark = ',')) %>% cat

# Block Service Points
#======================================================
cat('Assigning blocks... \n')

block_bins = c(seq(1, row_count, by = ceiling(row_count / argsv$blocks) + 1), Inf)
service_points[, block:= cut(.I, breaks = block_bins, labels = FALSE, include.lowest = TRUE)]

# Grouping Export Data
#======================================================
cat('Grouping and exporting data... \n')

extract_block_GEN = function(dat, k) {
  function(f) {
    fread(f, key = k, colClasses = c('service_point_id' = cfg$col_classes$sp)) %>% # << needs revision
      merge(dat[, .SD, .SDcols = k])
  }
} 

blocks = 1:argsv$blocks
for(b in blocks) {
  extract_block = extract_block_GEN(service_points[block == b], k = cfg$col_names$sp)
  block_dat = lapply(raw_files, extract_block) %>%
    rbindlist
  export_path = sprintf('data/blocked/block%s.csv', b)
  fwrite(block_dat, export_path)
}
