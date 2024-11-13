# Use the official R image as the base image
FROM rocker/r-ver:4.3.0

# Install system dependencies for R packages (spatial packages and others)
RUN apt-get update && apt-get install -y \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install remotes (needed to install specific versions of packages)
RUN R -e "install.packages('remotes', repos='https://cran.rstudio.com/')"

# Copy the install.R script into the container
COPY src/install.R /src/install.R

# Run the install.R script to install all the necessary R packages with specific versions
RUN Rscript /src/install.R

# Copy your main R script into the container
COPY src/points_att_polygon.R /src/points_att_polygon.R

# Set the working directory to /src
WORKDIR /src

# Specify the command to run the R script with arguments
ENTRYPOINT ["Rscript", "/src/points_att_polygon.R"]
