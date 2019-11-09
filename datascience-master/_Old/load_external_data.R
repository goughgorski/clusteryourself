
load_external_data <- function(connection, path = NULL) {}
#get a vector of all filenames
files <- list.files(path= path, pattern = "*.txt", full.names = TRUE, recursive = TRUE)

#get the directory names of these (for grouping)
dirs <- dirname(files)

#find the last file in each directory (i.e. latest modified time)
#lastfiles <- tapply(files,dirs,function(v) v[which.max(file.mtime(v))])

#load data as a list
external_data <- lapply(files, read.delim, sep = "|")

external_data <- lapply(files, read.delim, sep = "|", quote ="", row.names = NULL, stringsAsFactors = FALSE)
lapply(external_data, nrow)

