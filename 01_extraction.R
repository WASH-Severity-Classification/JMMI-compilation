library(extractRC)
library(readr)
library(lubridate)
library(purrr)
library(readxl)
library(tidyr)

search_URL <- "https://www.reachresourcecentre.info/search/?search=1&initiative%5B%5D=reach&ptype%5B%5D=dataset-database&dates=&keywords=JMMI"
all_URLs <- RC_extract_allPages(search_URL)

all_files_names_URL <- gsub("^.*\\/", "", all_URLs)

# all_data <-  download_files(all_URLs, "data")

iso3 <- read_csv("https://raw.githubusercontent.com/ElliottMess/pcodesOCHA/master/output/available_countries.csv")


iso3_pattern <- paste0(iso3$country_iso3code, "|", tolower(iso3$country_iso3code), collapse = "|") %>%
  paste0("|CAR|car")

sep_year_pattern <- "[.-_-]{0,2}[0-9]{2,4}"

clean_dates <- function(data, date_col){

  string_vec <- rlang::ensym(date_col)
  string_vec_value <- data %>%
    select(!!date_col)

  lower_abb_month_patt <- paste(paste0(tolower(month.abb),sep_year_pattern),
                                sep = "|",collapse = "|")
  abb_months_pattern <- paste0(month.abb, sep_year_pattern,
                              collapse = "|")
  all_abbs_months_patt <- paste(lower_abb_month_patt, abb_months_pattern,
                                sep = "|", collapse = "|")

  lower_full_month_patt <- paste(paste0(tolower(month.name),sep_year_pattern),
                                 sep = "|",collapse = "|")
  full_months_pattern <- paste0(month.name,sep_year_pattern, collapse = "|")
  all_full_months_patt <- paste(lower_full_month_patt, full_months_pattern,
                                sep = "|", collapse = "|")

  monthNames_patterns <- paste0(abb_months_pattern, "|",all_full_months_patt)

  months_names <- data.frame(month_name = c(month.abb, tolower(month.abb), month.name, tolower(month.name)),
                             month_id = rep(c(1:12),4))

  extract_date_string <- (lapply(string_vec_value, function(x){str_extract(x, monthNames_patterns)}))
  extract_date_string <- extract_date_string$file_name

  if(sum(is.na(extract_date_string))>0){
    nas <- string_vec_value$file_name[is.na(extract_date_string)]
    warning(paste0("Date could not be extracted from: ", nas, "\n"))
  }

  extract_month <- str_extract(extract_date_string, "[:alpha:]*")

  months_id <- unlist(lapply(extract_month,function(x){months_names$month_id[grepl(x, months_names$month_name)][1]}))

  extract_year <- str_extract(extract_date_string, "[0-9]{2,4}")
  extract_year <- unlist(lapply(extract_year, function(x){if_else(length(x) == 2, paste0("20", x), x)}))

  extract_date <- dmy(paste("01",months_id, extract_year, sep ="-"))

  dplyr::mutate(data, date = extract_date)

}

all_files_names <- data.frame(file_name = list.files("data/")) %>%
  mutate(iso3_code = toupper(str_extract(file_name, iso3_pattern)),
         file_path = paste0("data/", file_name)) %>%
  clean_dates("file_name")%>%
  filter(iso3_code != "NAM")

unique_countries <- unique(all_files_names$iso3_code)

sheets_data <- c("median_WASH_SMEB_district", "Median item prices","Price per item - district", "data", "Coût du PMAS", "median_price_USD")

countries_sheets <- data.frame(iso3_code = unique_countries, sheets_data=sheets_data)

all_files_names_sheets <- left_join(all_files_names, countries_sheets, by = "iso3_code") %>%
  mutate(data_name = paste0(str_remove(file_name, ".xlsx$"), "_", date),
         new_name = paste0("REACH_", iso3_code, "_JMMI_data_",date),
         new_name = case_when(duplicated(new_name)~ paste0(make.unique(new_name, sep = "_")),
                                   TRUE ~ new_name))

write_csv(all_files_names_sheets, "all_files_available.csv")

read_excel_all <- function(file_path, sheet){

  all_sheets <- excel_sheets(file_path)

  if(sheet %in% all_sheets){
    read_excel(file_path, sheet = sheet)
  }else{
    warning("oupsi, the sheet was not found in the file:", file_path)

  }
}


list_dfs <- map2(all_files_names_sheets$file_path, all_files_names_sheets$sheets_data, read_excel_all)

names(list_dfs) <- all_files_names_sheets$new_name

country_list <- unique(all_files_names_sheets$iso3_code)

single_csvs <- function(list_dfs, country){

  all_files_names_sheets <- readr::read_csv("all_files_available.csv", col_types = cols())%>%
    mutate(data_name = paste0(str_remove(file_name, ".xlsx$"), "_", date)) %>%
    filter( iso3_code == country)

  dfs_inCountry <- names(list_dfs)[names(list_dfs) %in% all_files_names_sheets$new_name]

  classes <- lapply(list_dfs, class) %>%
    bind_rows() %>%
    tidyr::pivot_longer(cols = everything(), names_to = "file_name", values_to = "class") %>%
    filter(class == "data.frame", grepl(country, file_name))

  list_dfs_country <- list_dfs[names(list_dfs) %in% classes$file_name]

  if(!dir.exists("outputs")){
    dir.create("outputs")
  }
  if(!dir.exists(paste0("outputs/", country))){
    dir.create(paste0("outputs/", country))
  }
  name_files <- paste0("outputs/", country, "/",names(list_dfs_country), ".csv")
  map2(list_dfs_country, name_files, write_csv)

}


for(i in 1:length(country_list)){
  single_csvs(list_dfs, country_list[i])
}
