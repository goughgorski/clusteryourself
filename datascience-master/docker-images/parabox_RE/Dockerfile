# Use an official Heroku runtime as a parent image
FROM heroku/heroku:18

# Install packages
RUN apt update && \
    apt-get update && \
    apt-get --assume-yes install software-properties-common && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    add-apt-repository 'deb [arch=amd64,i386] https://cran.rstudio.com/bin/linux/ubuntu bionic-cran35/' && \
    apt-get update && \
    apt-get --assume-yes install r-base-core r-base-dev r-base libpq-dev libxml2-dev libcurl4-openssl-dev libssl-dev && \
    R --version

# Set the working directory to /app
WORKDIR /app

#install R packages
RUN R -e 'install.packages("Rserve",repos="https://www.rforge.net/",Ncpus=4)'
    
COPY parabox_packages.R /app/
RUN R -e 'install.packages(setdiff(source("/app/parabox_packages.R")$value, installed.packages()),Ncpus=4)'

# Copy the current directory contents into the container at /app
COPY . /app/

# Make port $PORT available to the world outside this container
EXPOSE ${PORT:-8080}

# Define environment variable
ENV R_HOME /usr/lib/R

CMD R CMD Rserve --RS-conf /app/Rserv.conf --RS-set http.port=${PORT:-8080} --vanilla 
