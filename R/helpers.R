process_join_table <- function(join_table) {

  join_table <- join_table %>%
    mutate(source_combo = paste(source1_name, source2_name),
           source1_field = paste(source1_name, source1_field, sep = "."),
           source2_field = paste(source2_name, source2_field, sep = "."))

  for(i in 1:dim(join_table)[1]) {
    # i = 2
    field <- join_table[i, "source1_field"]
    match_subset <- join_table %>%
      filter(source2_field == field)
        if(dim(match_subset)[1] == 1) {
          join_table[i, "source1_field"] <- match_subset[["source1_field"]]
        }
      }

  return(join_table)
}

convert_white_to_NA <- function(df) {
  for(i in names(df)) {
    if(class(df[[i]]) == "character") {
      df[[i]][!grepl("\\S", df[[i]])] <- NA
    }
  }
  return(df)
}



# join_table <- read.table(header = TRUE, stringsAsFactors = FALSE, text = '
# source1_name  source1_field  source2_name  source2_field  type
# subject       subject_id     case          patient_id     left
# case          patient_id     specimen      subject_mrn    left
# case          visit          specimen      event          left
# specimen      subject_mrn    annotation    guy_d          left
# specimen      event          annotation    time_point     left
# specimen      specimen_id    annotation    barcode        left
# case          patient_id     map_case      case_subj_id   left
#                     ')
