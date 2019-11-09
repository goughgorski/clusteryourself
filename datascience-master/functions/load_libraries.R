load_libraries <- function(func_dir, include_bundles = 'all', exclude_bundles = NULL) {
 
  #Group packages into bundles
  utility <- c("plyr"
              ,"dplyr" 
              ,"RPostgreSQL"
              ,"reshape2"
              ,"base64enc"
              ,"devtools"
              ,"jsonlite"
              ,"parallel"
              ,"lubridate"
              ,"svMisc"
              ,"aws.s3"
              )

  productivity <- c("stats")

  network <- c()

  text <- c("tm"
            ,"text2vec"
            ,"stm")

  modeling <- c("randomForest"
                , "glmnet"
                , "mfp")

  #Get list of selected bundles
  if (include_bundles == 'all') {bundles <- c('utility', 'productivity', 'network', 'text', 'modeling')} else {bundles <- include_bundles}
  if (!is.null(exclude_bundles)){
    bundles <- bundles[!bundles %in% exclude_bundles]
  }

  list.of.packages <- c()
  for (i in 1:length(bundles)) {
    list.of.packages <- c(list.of.packages, eval(parse(text=bundles[i])))
  }

  #Install packages if they don't exist
  new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
  if(length(new.packages)) {install.packages(new.packages)}
  
  #Handle Dev Tool Packages
  utility <- c()
  text <- c()
  modeling <- c('rstudio/tensorflow')
  if (include_bundles == 'all') {bundles <- c('utility', 'text', 'modeling')} else {bundles <- include_bundles}
  if (!is.null(exclude_bundles)){
    bundles <- bundles[!bundles %in% exclude_bundles]
  }

  dev.packages <- c()
  for (i in 1:length(bundles)) {
    library(devtools)
    dev.packages <- c(dev.packages, eval(parse(text=bundles[i])))
  }

  dev.package.names <- regexpr('/.*',dev.packages, perl = TRUE)
  dev.package.names <- substr(dev.packages,dev.package.names+1,dev.package.names + attr(dev.package.names,"match.length"))

  new.dev.packages <- dev.packages[!(dev.package.names %in% rownames(installed.packages()))]
  if(length(new.dev.packages)) {
    library(devtools)
    lapply(new.dev.packages, function(x){devtools::install_github(x)})
    }
  
  #Load New Packages
  inst = lapply(list.of.packages, library, character.only = TRUE)
  inst = lapply(dev.package.names, library, character.only = TRUE)

  if ('modeling' %in% bundles){ install_tensorflow() }

  source(paste(func_dir, 'sourceDir.R', sep=''))
  sourceDir(func_dir)

  }

# Trimmed from standard package loads

#   network <- c("igraph"
#               ,"network"
#               ,"sna"
#               ,"visNetwork"
#               ,"threejs"
#               ,"networkD3"
#               ,"ndtv"
#               ,"rgexf"
#               )
  
#   modeling <- c("FactoMineR"
#                 , "caret")

#   text <- c("NLP"
#               ,"text2vec"
#               ,"SnowballC"
#               )

#   productivity <- c("knitr"
#             ,"tidyr")

#   utility <- c("base64url"
#               ,"svMisc"
#               ) 

# Handle Dev Tool Packages
# utility <- c('berndbischl/parallelMap')
# text <- c('bmschmidt/wordVectors') 