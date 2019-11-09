corpus_to_matrix <- function(corpus, weighting = weightTF, fnw = 0, colnames = NULL, sfix = '', dname, type = 'tdm') {
  
  #Freqency dropout rate
  corpus_p <- round(fnw * length(corpus))

  #TDM
  print("Creating Term Document Matrix")
  corpus_tdm <- as.matrix(TermDocumentMatrix(corpus, 
                          control = list(weighting = weighting,
                                        bounds = list(global = c(corpus_p, Inf)))))
  
  #Add Message ID to column names
  if (!is.null(colnames)) {
    colnames(corpus_tdm) <- colnames
  }

  #Add stubs to word names
  rownames(corpus_tdm) <- paste(rownames(corpus_tdm),sfix, sep = '_')

  if (type == 'dtm') {
    print("Creating Document Term Matrix")
    #Create DTMs
    corpus_dtm <- t(corpus_tdm)
  }

}