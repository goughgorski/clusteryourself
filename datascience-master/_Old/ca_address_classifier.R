#Clear Environment
rm(list=ls())

#Load libraries
source('~/workspace/datascience/functions/load_libraries.R')
load_libraries('~/workspace/datascience/functions/')

#Set Seed
set.seed(123456789)

x_data <- runif(100, min=0, max=1)
y_data <- x_data * 0.1 + 0.3