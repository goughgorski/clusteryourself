strip_rf = function(randomForest) {
  
  randomForest$predicted <- c()
  randomForest$confusion <- c()
  randomForest$votes <- c()
  randomForest$oob.times <- c()
  randomForest$classes <- c()
  randomForest$importanceSD <- c()
  randomForest$localImportance <- c()
  randomForest$proximity <- c()
  randomForest$y <- c()
  randomForest$test <- c()
  randomForest$inbag <- c()

  return(randomForest)
  
  }