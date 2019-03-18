# Set Up Spark Connection

library(sparklyr)
library(dplyr)
spark_install()
sc <- spark_connect(master = "local")

# Load Transaction CSV files directly to spark

claim <- spark_read_csv(sc, "claim", "Data set/claims/Claim_2019.csv")

# Checking spark "claim"

sdf_dim(claim); sdf_nrow(claim); sdf_ncol(claim)

glimpse(claim)

print(claim, n = 2, width = Inf)

head(claim)

#Save merged file in Spark

claim <-sdf_coalesce(claim, partitions = 1)

spark_write_csv(claim,"Data set/claim csv and feather/", 
                header=TRUE ,
                charset = "UTF-8" ,  
                mode = "overwrite")

library(feather)

install.packages("rio")
library("rio")


z <- read.csv("Data set/claim csv and feather/claim.csv")
export(z, "Data set/claim csv and feather/claim.csv")
convert("Data set/claim csv and feather/claim.csv" , "Data set/claim csv and feather/claim.feather")
