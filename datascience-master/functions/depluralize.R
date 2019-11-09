# coarse singularization function for use in tokenization ** need to save to functions library **
depluralize <- function(term){
  #if (class(x) != 'character') {stop('depluralize only works on character values')}
  if (is.na(term)){NA
  } else if (term == "" | term == " ") {""
  } else if (class(term) != 'character') {term
  } else if (last(strsplit(term, '')[[1]]) == 's' & length(strsplit(term, '')[[1]]) > 2
             && strsplit(term, '')[[1]][length(strsplit(term, '')[[1]]) - 1] != 's'){
    char <- strsplit(term, '')[[1]]
    t_length <- length(char)
    if (char[t_length - 1] == 'e') {
      if (char[t_length - 2] == 'i') {
        paste(paste(char[1:(t_length - 3)], collapse = ''), 'y', sep = '')
      } else if (char[t_length - 2] %in% c('h', 'x', 'o') |
                 char[t_length - 2] == 's' & !char[t_length -3] %in% c('a', 'e', 'i', 'o', 'u')) {
        paste(char[1:(t_length - 2)], collapse = '')
      } else {paste(char[1:(t_length - 1)], collapse = '')}
    } else {paste(char[1:(t_length - 1)], collapse = '')}
  } else {term}
  
}