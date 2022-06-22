args = commandArgs(trailingOnly=TRUE)
library(sparklyr)
library(RPostgres)

# want to forecast :

print_prediction <- function(path_to_model, country_of_arrival, mean_of_transport,
                             custom, net_price, net_price_per_unit){
  
  if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
    Sys.setenv(SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.2.1/libexec/")
  }
  Sys.setenv(JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home") 
  
  sc <- spark_connect(master = "local")
  fitted <- ml_load(sc, path_to_model)
  newdata <- data.frame(price_transport_net=NA,
                        country_of_arrival = country_of_arrival,
                        mean_of_transport=mean_of_transport,
                        custom=custom,
                        net_price=net_price,
                        net_price_per_unit=net_price_per_unit)
  newdata_tbl <- sdf_copy_to(sc, newdata, overwrite = T)
  
  predictions <- ml_transform(x = fitted, dataset = newdata_tbl) %>% 
    select(prediction) %>% collect() %>% as.numeric()
  cat(paste("The prediction for these values is: ", round(predictions, 2)))
  #return (predictions)
}

print_prediction(args[1], args[2], args[3], args[4], args[5], args[6])
# print_prediction(country_of_arrival = "PANAMA", 
#                  mean_of_transport='AVION', custom='118', net_price='12345', 
#                  net_price_per_unit='123')

