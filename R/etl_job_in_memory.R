etl_job_in_memory <- setRefClass("etl_job_in_memory",
                                 contains = "etl_job",
                                 fields = c("source_tables", "merged_table"))

etl_job_in_memory$methods(
  source_data = function(supported_types = "csv") {
    # sources <- j$source
    sources <- .self$source
    types <- sapply(sources, function(x) x$type)

    if(sum(!types %in% supported_types) > 0) {
      stop(paste("no support for source type(s)", types[!types %in% supported_types]))
    }

    tables <- lapply(sources, function(source) {
      # source <- sources[[5]]
      if(source$type == "csv"){
        table <- source_csv(source$location, source$fields$field,
                            source$fields$field_type)
        for(i in names(table)){
          if(class(table[[i]]) == "character"){
            table[[i]][!grepl("\\S", table[[i]])] <- NA
            table[[i]] <- stringr::str_trim(table[[i]], "both")
          }
        }
      }
      return(table)
    })

    .self$source_tables <- tables

  }

)

etl_job_in_memory$methods(
  filter_sources = function() {
    filters <- .self$filter
    # filters <- j$filter

    source_names <- names(.self$source)
    # source_names <- names(j$source)

    filters$source <- stringr::str_trim(filters$source, "both")
    filters <- filters %>%
      filter(!is.na(source))

    if(sum(!filters$source %in% source_names) > 0) {
      stop(paste(filters$source[!filters$source %in% source_names], "not a source"))
    }

    for(i in 1:dim(filters)[1]) {

      source_name <- filters[i, "source"]
      source_group_by <- filter[i, "group_by"]
      source_filter <- filters[i, "filter"]

      if(!is.na(source_group_by)){
        .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
          group_by_(source_group_by)
      }
      .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
        filter_(source_filter)

    }
  }
)

etl_job_in_memory$methods(
  reshape_sources = function() {
    # .self <- j
    reshape_table <- .self$reshape %>%
      filter(!is.na(source))

    for(i in 1:dim(reshape_table)[1]) {
      # i = 2
      source_name <- reshape_table[i, "source"]
      reshape_type <- reshape_table[i, "type"]
      key <- reshape_table[i, "key"]
      value <- reshape_table[i, "value"]
      if(grepl("gather", reshape_type)) {
        fields <- reshape_table[i, "fields"]
        fields <- strsplit(fields, "|", fixed = TRUE)[[1]]
        .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
          gather_(key_col = key, value_col = value, gather_cols = fields)
      } else if(grepl("spread", reshape_type)) {
        .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
          spread_(key_col = key, value_col = value)
        names(.self$source_tables[[source_name]]) <- stringr::str_trim(names(.self$source_tables[[source_name]]), "both")
        names(.self$source_tables[[source_name]]) <- gsub("\\s", "_", names(.self$source_tables[[source_name]]))
      } else {
        stop(paste(reshape_type, "not a valid reshaping function"))
      }
    }
  }
)

etl_job_in_memory$methods(
  join_tables = function() {

    # .self <- j
    joins_table <- etljobs:::process_join_table(.self$join)

    join_sets <- joins_table %>%
      select(source_combo, source1_name, source2_name, type) %>%
      distinct() %>%
      filter(!is.na(type))

    source_names <- unique(c(join_sets$source1_name, join_sets$source2_name))
    if(sum(!source_names %in% names(.self$source_tables)) > 0){
      stop(paste(source_names[!source_names %in% names(.self$source_tables)],
                 "not source table(s)"))
    }
    types <- unique(join_sets$type)
    if(sum(!types %in% c("left", "right", "full", "semi", "anti")) > 0) {
      stop(paste(types[!types %in% c("left", "right", "full", "semi", "anti")],
           "not valid join type(s)"))
    }

    for(k in source_names) {
      names(.self$source_tables[[k]]) <- paste(k, names(.self$source_tables[[k]]),
                                               sep = ".")
    }

    # for(k in source_names) {
    #   # k <- source_names[1]
    #   names(j$source_tables[[k]]) <- paste(k, names(j$source_tables[[k]]), sep = ".")
    # }

    join <- left_join(join_sets[1, ], joins_table,
                      c("source_combo", "source1_name", "source2_name", "type"))

    join_by <- join[, "source2_field"]

    names(join_by) <- join[, "source1_field"]

    join_function <- eval(parse(text = paste0(unique(join[, "type"]), "_join")))

    .self$merged_table <- join_function(.self$source_tables[[unique(join$source1_name)]],
                                        .self$source_tables[[unique(join$source2_name)]],
                                        join_by)

    .self$source_tables[[unique(join$source1_name)]] <- NULL
    .self$source_tables[[unique(join$source2_name)]] <- NULL

    # combined_table <- join_function(j$source_tables[[join$source1_name]],
    #                                 j$source_tables[[join$source2_name]],
    #                                 join_by)

    if(dim(join_sets)[1] > 1){
      for(i in 2:dim(join_sets)[1]) {
        # i = 2
        join <- left_join(join_sets[i, ], joins_table,
                          c("source_combo", "source1_name", "source2_name", "type"))

        join_by <- join[, "source2_field"]

        names(join_by) <- join[, "source1_field"]

        join_function <- eval(parse(text = paste0(unique(join[, "type"]), "_join")))

        .self$merged_table <- join_function(.self$merged_table,
                                            .self$source_tables[[unique(join$source2_name)]],
                                            join_by)
        .self$source_tables[[unique(join$source2_name)]] <- NULL
      }
    }
  }
)

etl_job_in_memory$methods(
  transform_table = function() {
    # .self <- j
    for(i in 1:dim(.self$transform)[1]){
      # i = 1
      .self$merged_table <- .self$merged_table %>%
        mutate_(.dots = paste(.self$transform[i, "new_field"],
                      .self$transform[i, "transformation"], sep = " = "))

      x <- .self$merged_table
      x <- x %>%
        mutate_("subject = subject.subject_id")
    }
  }
)
