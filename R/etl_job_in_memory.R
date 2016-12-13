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
  join_tables = function() {

    # .self <- j
    joins_table <- .self$join %>%
      mutate(source_combo = paste(source1_name, source2_name))

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

    join <- left_join(join_sets[1, ], joins_table, c("source_combo"))

    join_by <- paste(join[1, "source1_name"], joins_table[["source1_field"]],
                         sep = ".")
    right_fields <- paste(join[1, "source2_name"], joins_table[["source2_field"]],
                     sep = ".")
    names(join_by) <- right_fields

    join_function <- eval(parse(text = paste0(join[1, "type"], "_join")))

    combined_table <- join_function(.self$source_tables[[join$source1_name]],
                                    .self$source_tables[[join$source2_name]],
                                    join_by)

    # combined_table <- join_function(j$source_tables[[join$source1_name]],
    #                                 j$source_tables[[join$source2_name]],
    #                                 join_by)

    if(dim(join_sets)[1] > 1){
      for(i in 2:dim(join_sets)[1]) {
        # i = 1
        join <- left_join(join_sets[i, ], joins_table, c("source1_name", "source2_name"))

        join_by <- paste(join[i, "source1_name"], joins_table[["source1_field"]],
                         sep = ".")
        right_fields <- paste(join[i, "source2_name"], joins_table[["source2_field"]],
                              sep = ".")
        names(join_by) <- right_fields

        join_function <- eval(parse(text = paste0(joins[i, "type"], "_join")))

        combined_table <- join_function(combined_table,
                                        .self$source_tables[[join$source2_name]],
                                        join_by)
      }
    }

    .self$merged_table <- combined_table
  }
)
