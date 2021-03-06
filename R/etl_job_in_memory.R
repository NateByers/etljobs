etl_job_in_memory <- setRefClass("etl_job_in_memory",
                                 contains = "etl_job",
                                 fields = c("connections", "source_tables",
                                            "output_table"))

etl_job_in_memory$methods(
  make_connections = function() {
    #.self <- j
    .self$connections <- apply(.self$connect, 1, function(row) {
      # row <- .self$connect[1, ]
      if(tolower(row["type"]) == "odbc"){
        dsn <- row["name"]
        usr <- row["username"]
        if(is.na(usr)) {
          usr <- grep(paste0(toupper(dsn), "_USR"), names(Sys.getenv()), value = TRUE)
          if(length(usr) == 0) {
            usr <- ""
          } else {
            usr <- Sys.getenv(usr)
          }
        }
        pwd <- row["password"]
        if(is.na(pwd)) {
          pwd <- grep(paste0("^", toupper(dsn), "_PWD"), names(Sys.getenv()), value = TRUE)
          if(length(pwd) == 0) {
            pwd <- ""
          } else {
            pwd <- Sys.getenv(pwd)
          }
        }
        con <- pass_parameters(func = "odbcConnect", .dots = list(dsn=dsn, uid = usr, pwd = pwd))
        con <- odbcConnect(dsn = dsn, uid = usr, pwd = pwd)
        return(list(type = "odbc", dsn = dsn, connection = con))
      }
    })
  }
)

etl_job_in_memory$methods(
  source_data = function(supported_types = "csv") {
    # .self <- j
    sources <- .self$source
    types <- sapply(sources, function(x) x$type)
    
    if(sum(!types %in% supported_types) > 0) {
      stop(paste("no support for source type(s)", types[!types %in% supported_types]))
    }
    
    tables <- lapply(sources, function(source) {
      # source <- sources[[1]]
      if(source$type == "csv"){
        table <- source_csv(source$source, source$location, source$fields$field,
                            source$fields$field_type) %>%
          process_char_columns()
      }
      return(table)
    })
    
    .self$source_tables <- tables
    
  }
  
)

etl_job_in_memory$methods(
  filter_sources = function() {
    # .self <- j
    
    filter_table <- .self$filter
    
    source_names <- names(.self$source)
    # source_names <- names(j$source)
    
    filter_table <- filter_table %>%
      dplyr::filter(!is.na(source))
    
    if(dim(filter_table)[1] > 0) {
      
      if(sum(!filter_table$source %in% source_names) > 0) {
        stop(paste(filter_table$source[!filter_table$source %in% source_names], "not a source"))
      }
      
      for(i in 1:dim(filter_table)[1]) {
        
        # i = 1
        source_name <- filter_table[i, "source"]
        source_filter <-filter_table[i, "filter"]
        
        if(!is.na(filter[i, "group_by"])){
          source_group_by <- as.list(strsplit(filter[i, "group_by"], "\\|")[[1]])
          .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
            dplyr::group_by_(.dots = source_group_by)
        }
        .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
          dplyr::filter_(source_filter) %>%
          dplyr::ungroup() %>%
          dplyr::as.data.frame()
        
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  recode_sources = function() {
    # .self <- j
    for(i in 1:length(.self$source_tables)) {
      # i <- 1
      for(j in names(.self$source_tables[[i]])) {
        # j <- "demo.PTGENDER"
        if(j %in% .self$recode$field) {
          recode_table <- .self$recode %>%
            dplyr::filter(field == j)
          .self$source_tables[[i]][[j]] <- as.character(.self$source_tables[[i]][[j]])
          .self$source_tables[[i]][[j]][.self$source_tables[[i]][[j]] %in% recode_table[["code"]]] <- recode_table[match(.self$source_tables[[i]][[j]][.self$source_tables[[i]][[j]] %in% recode_table[["code"]]], recode_table[, "code"]), "recode_value"]
        }
      }
    }
  }
)

etl_job_in_memory$methods(
  reshape_sources = function() {
    # .self <- j
    reshape_table <- .self$reshape %>%
      dplyr::filter(!is.na(source))
    
    if(dim(reshape_table)[1]) {
      
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
            dplyr::gather_(key_col = key, value_col = value, gather_cols = fields) %>%
            as.data.frame()
          
        } else if(grepl("spread", reshape_type)) {
          
          .self$source_tables[[source_name]] <- .self$source_tables[[source_name]] %>%
            dplyr::spread_(key_col = key, value_col = value) %>%
            as.data.frame()
          names(.self$source_tables[[source_name]]) <- stringr::str_trim(names(.self$source_tables[[source_name]]), "both")
          names(.self$source_tables[[source_name]]) <- gsub("\\s", "_", names(.self$source_tables[[source_name]]))
          
        } else {
          
          stop(paste(reshape_type, "not a valid reshaping function"))
          
        }
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  join_tables = function() {
    
    # .self <- j
    
    if(dim(.self$join)[1] > 0) {
      
      joins_table <- process_join_table(.self$join)
      
      join_sets <- joins_table %>%
        dplyr::select(source_combo, source1_name, source2_name, type) %>%
        dplyr::distinct() %>%
        dplyr::filter(!is.na(type))
      
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
      
      # for(k in source_names) {
      #   # k <- source_names[1]
      #   names(j$source_tables[[k]]) <- paste(k, names(j$source_tables[[k]]), sep = ".")
      # }
      
      join_ <- dplyr::left_join(join_sets[1, ], joins_table,
                                c("source_combo", "source1_name", "source2_name", "type"))
      
      join_by <- join_[, "source2_field"]
      
      names(join_by) <- join_[, "source1_field"]
      
      join_function <- eval(parse(text = paste0("dplyr::", unique(join_[, "type"]), "_join")))
      
      .self$output_table <- join_function(.self$source_tables[[unique(join$source1_name)]],
                                          .self$source_tables[[unique(join$source2_name)]],
                                          join_by) %>%
        as.data.frame()
      
      .self$source_tables[[unique(join$source1_name)]] <- NULL
      .self$source_tables[[unique(join$source2_name)]] <- NULL
      
      # combined_table <- join_function(j$source_tables[[join$source1_name]],
      #                                 j$source_tables[[join$source2_name]],
      #                                 join_by)
      
      if(dim(join_sets)[1] > 1){
        for(i in 2:dim(join_sets)[1]) {
          # i = 2
          join_ <- dplyr::left_join(join_sets[i, ], joins_table,
                                    c("source_combo", "source1_name", "source2_name", "type"))
          
          join_by <- join_[, "source2_field"]
          
          names(join_by) <- join_[, "source1_field"]
          
          join_function <- eval(parse(text = paste0("dplyr::", unique(join_[, "type"]), "_join")))
          
          .self$output_table <- join_function(.self$output_table,
                                              .self$source_tables[[unique(join$source2_name)]],
                                              join_by) %>%
            as.data.frame()
          .self$source_tables[[unique(join$source2_name)]] <- NULL
        }
      }
      
    } else {
      if(length(.self$source_tables) > 1) {
        stop("must provide information in join table for merging sources")
      }
      .self$output_table <- .self$source_tables[[1]]
    }
    
  }
)

etl_job_in_memory$methods(
  transform_table = function() {
    # .self <- j
    
    trans_table <- .self$transform
    
    if(dim(trans_table)[1] > 0) {
      
      for(i in 1:dim(trans_table)[1]) {
        
        # i = 6
        
        trans_filter <- list(trans_table[i, "transformation"])
        names(trans_filter) <- trans_table[i, "new_field"]
        
        if(!is.na(trans_table[i, "group_by"])) {
          
          trans_group_by <- as.list(strsplit(trans_table[i, "group_by"], "\\|")[[1]])
          .self$output_table <- .self$output_table %>%
            dplyr::group_by_(.dots = trans_group_by)
          
        }
        
        .self$output_table <- .self$output_table %>%
          dplyr::mutate_(.dots = trans_filter) %>%
          dplyr::ungroup() %>%
          as.data.frame()
        
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  filter_output = function() {
    # .self <- j
    filter_table <- .self$filter %>%
      dplyr::filter(is.na(source))
    
    if(dim(filter_table)[1] > 0) {
      
      for(i in 1:dim(filter_table)[1]) {
        
        output_filter <- filter_table[i, "filter"]
        
        if(!is.na(filter_table[i, "group_by"])) {
          
          output_group_by <- as.list(strsplit(filter_table[i, "group_by"], "\\|")[1])
          .self$output_table <- .self$output_table %>%
            dplyr::group_by_(.dots = output_group_by)
          
        }
        
        .self$output_table <- .self$output_table %>%
          dplyr::filter_(output_filter) %>%
          dplyr::ungroup() %>%
          as.data.frame()
        
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  summarize_output = function() {
    
    # .self <- j
    sum_table <- .self$summarize
    
    if(dim(sum_table)[1] > 0) {
      
      for(i in 1:dim(sum_table)[1]) {
        # i = 1
        output_summary <- list(sum_table[i, "summarize"])
        names(output_summary) <- sum_table[i, "new_field"]
        
        if(!is.na(sum_table[i, "group_by"])) {
          
          sum_group_by <- as.list(strsplit(sum_table[i, "group_by"], "\\|")[[1]])
          .self$output_table <- .self$output_table %>%
            dplyr::group_by_(.dots = sum_group_by)
          
        }
        
        .self$output_table <- .self$output_table %>%
          dplyr::summarize_(.dots = output_summary) %>%
          dplyr::ungroup() %>%
          as.data.frame()
        
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  reshape_output = function() {
    # .self <- j
    reshape_table <- .self$reshape %>%
      dplyr::filter(is.na(source))
    
    if(dim(reshape_table)[1] > 1) {
      stop("cannot reshape output more than once")
    } else if(dim(reshape_table)[1] == 1) {
      
      reshape_type <- reshape_table[, "type"]
      key <- reshape_table[, "key"]
      value <- reshape_table[, "value"]
      
      if(grepl("gather", reshape_type)) {
        
        fields <- reshape_table[, "fields"]
        fields <- strsplit(fields, "|", fixed = TRUE)[[1]]
        .self$output_table <- .self$output_table %>%
          tidyr::gather_(key_col = key, value_col = value, gather_cols = fields) %>%
          as.data.frame()
        
      } else if(grepl("spread", reshape_type)) {
        
        .self$output_table <- .self$output_table %>%
          tidyr::spread_(key_col = key, value_col = value) %>%
          as.data.frame()
        names(.self$output_table) <- stringr::str_trim(names(.self$output_table), "both")
        names(.self$output_table) <- gsub("\\s", "_", names(.self$output_table))
        
      } else {
        
        stop(paste(reshape_type, "not a valid reshaping function"))
        
      }
      
    }
    
  }
)

etl_job_in_memory$methods(
  
  load_data = function() {
    # .self <- j
    type <- unique(.self$load$type[!is.na(.self$load$type)])
    if(length(type) > 1) stop("load type can only have one value")
    
    endpoint <- unique(.self$load$endpoint[!is.na(.self$load$endpoint)])
    if(length(endpoint) > 1) stop("load endpoint can only have one value")
    if(length(endpoint) == 0) stop("load must have an endpoint value")
    
    append <- unique(.self$load$append[!is.na(.self$load$append)])
    if(length(append) > 1) stop("load append can only have one value")
    if(length(append) == 0) append <- FALSE
    
    fields <- unique(.self$load$fields[!is.na(.self$load$fields)])
    if(length(fields) > 1) stop("load fields should only have one value, with fields separated by |")
    if(length(fields) == 0) {
      fields <- unique(.self$transform$new_field)
    } else {
      fields <- strsplit(fields, "\\|")[[1]]
    }
    
    .self$output_table <- .self$output_table %>%
      dplyr::select_(.dots = fields) %>%
      dplyr::distinct()
    
    if(tolower(type) == "csv") {
      load_csv_in_memory(.self$output_table, endpoint, fields, append)
    }
    if(tolower(type) == "odbc") {
      load_odbc_in_memory(.self$output_table, endpoint, table = .self$load$table,
                          schema = .self$load$schema, append)
    }
  }
  
)

