# Set Up Spark Connectio

library(sparklyr)
library(dplyr)
spark_install()
sc <- spark_connect(master = "local")

# Load Transaction CSV files directly to spark

total_transaction <- spark_read_csv(sc, "total_transaction", "Data set/Transaction Datasets/*.CSV")

# Checking spark "total_transaction"

sdf_dim(total_transaction); sdf_nrow(total_transaction); sdf_ncol(total_transaction)

glimpse(total_transaction)

print(total_transaction, n = 2, width = Inf)

head(total_transaction)

#Save merged file in Spark

total_transaction <-sdf_coalesce(total_transaction, partitions = 1)

spark_write_csv(total_transaction,"Data set/Merged Transaction CSV", 
                header=TRUE ,
                charset = "UTF-8" ,  
                mode = "overwrite")

library(feather)

install.packages("rio")
library("rio")


s <- read.csv("Data set/Merged Transaction CSV/total_transaction.csv")
export(s, "Data set/Merged Transaction CSV/total_transaction.csv")
convert("Data set/Merged Transaction CSV/total_transaction.csv" , "Data set/Merged Transaction CSV/total_transaction.feather")
