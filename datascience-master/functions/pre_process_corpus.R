pre_process_corpus <- function(data, text, stopword_lang = 'english', extra_stopwords = NULL, non_stopwords = NULL, remove_letters = FALSE, stem = FALSE, stem_dict = NULL) {
  
  #Casing
  print(noquote("Standardizing casing"))
  text <- tolower(data[,text])
  
  #Remove unprintable Characters
  print(noquote("Removing non-printable characters"))  
  text <- gsub('^[:print:]','',text)
  
  #Remove non-ascii characters
  print(noquote("Removing non-ascii characters"))
  text <- gsub("[^\001-\177]",'', text, perl = TRUE)
  
  #Convert to volatile corpus
  print(noquote("Converting to VCorpus"))  
  text <- VCorpus(VectorSource(text))
  
  #Set and Remove Stopwords
  print(noquote("Removing stopwords"))  
  
  stopwords <- c(stopwords(stopword_lang),extra_stopwords)
  stopwords <- stopwords[which(!stopwords %in% non_stopwords)]
  
  text <- tm_map(text, function(x) {removeWords(x,stopwords)})
  
  #Add Entity extraction here ?
  
  #Remove punctuation, Strip Whitespace, Remove Numbers
  print(noquote("Removing punctuation, whitespace, numbers, and letter-words"))        
  text <- tm_map(text, function(x) {removePunctuation(x)})
  text <- tm_map(text, function(x) {stripWhitespace(x)})
  text <- tm_map(text, function(x) {removeNumbers(x)})
  
  if (remove_letters) {
    text <- tm_map(text, function(x) {removeWords(x,letters)})
  }
  
  if (stem == TRUE) {
    print(noquote("Stemming words"))        
    text <- tm_map(text, function(x) {stemDocument(x)})
    if (!is.null(stem_dict)) {
      text <- tm_map(text, function(x) {stemCompletion(x, dictionary = stem_dict)})
    } else { print(noquote("No stem completion dictionary found. Leaving stems as is."))}
  }
  
  return(text)
}