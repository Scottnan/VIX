
# update data

option_info_name <- "option_info.feather"

option_trade_name <- "option_trade.feather"

yiled_curve_name <- "yiled_curve.feather"

update_options <- function(from_to, .folder) {

  from_to <- GCAMCQT::as_from_to(from_to)

  # info
  w_wind_data <- w.wset('optioncontractbasicinfo','exchange=sse;windcode=510050.SH;status=all;
                         field=wind_code,sec_name,call_or_put,exercise_price,listed_date,expire_date')
  option_infos <- w_wind_data$Data %>% as.data.table()
  option_infos[, `:=` (listed_date = as.Date(listed_date, origin = "1899-12-30"),
                       expire_date = as.Date(expire_date, origin = "1899-12-30"),
                       CODE = NULL)]
  update_option_infos(option_infos, .folder)

  # trading
  trade_wind_code <- paste0('startdate=', as.character(from_to$from),
                            ';enddate=', as.character(from_to$to), ';exchange=sse;windcode=510050.SH;',
                            'field=option_code,date,open,highest,lowest,close,settlement_price,volume,amount,',
                            'position')
  option_trade <- w.wset('optiondailyquotationstastics', trade_wind_code)$Data %>% as.data.table()
  option_trade[, `:=` (date = as.Date(date, origin = "1899-12-30"),
                       CODE = NULL)]
  update_option_trade(option_trade, .folder)

  # yield_curve
  yield_ <- w.edb('M1004179,M1004696,M1001099,M1001100,M1001101,M1001102', as.character(from_to$from),
                  as.character(from_to$to), 'Fill=Previous')$Data %>% as.data.table()
  yield_[, DATETIME := as.Date(DATETIME, origin="1970-01-01")]
  setnames(yield_, c("DATE", "0M", "1M", "3M", "6M", "9M", "12M"))

  update_yield_curve(yield_, .folder)
}


update_option_infos <- function(option_infos, .folder) {

  path <- paste0(.folder, option_info_name)
  setkey(option_infos, wind_code)
  stopifnot(GCAMCQT::is_pk_dt(option_infos))
  GCAMCPUB::writeDtFeather(option_infos, path)
}


update_option_trade <- function(option_trade, .folder) {

  path <- paste0(.folder, option_trade_name)
  if (file.exists(path)) {
    origin_option_trade <- GCAMCPUB::readDtFeather(path)
    option_trade <- rbind(origin_option_trade, option_trade) %>% unique(.)
  }
  setkey(option_trade, option_code, date)
  stopifnot(GCAMCQT::is_pk_dt(option_trade))
  GCAMCPUB::writeDtFeather(option_trade, path)
}

update_yiled_curve <- function(yield_, .folder) {

  path <- paste0(.folder, yiled_curve_name)
  setkey(yiled_, DATE)
  stopifnot(GCAMCQT::is_pk_dt(yiled_))
  GCAMCPUB::writeDtFeather(yiled_, path)
}


