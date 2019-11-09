strip_model = function(model) {
  
  model$y = c()
  model$model = c()
  model$residuals = c()
  model$fitted.values = c()
  model$data = c()
  model$effects = c()
  model$df.residual = c()
  model$assign = c()
  model$qr$qr = c()  
  model$qr$qraux = c()  
  model$qr$tol = c()
  attr(model$terms,".Environment") = c()
  attr(model$formula,".Environment") = c()
  model$na.action = c()

  return(model)
  
  }