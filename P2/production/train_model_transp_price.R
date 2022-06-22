args = commandArgs(trailingOnly=TRUE)
# library(RPostgres)
# library(dplyr)
# library(sparklyr)



db <- 'dataimporta' 
db_port <- '5432'  # or any other port specified by the DBA
db_user <- 'pietro'  
db_password <- 'ropby8pietro'
con <- dbConnect(RPostgres::Postgres(), dbname = db, port=db_port, user=db_user, password=db_password)  


if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/homebrew/Cellar/apache-spark/3.2.1/libexec/")
}
Sys.setenv(JAVA_HOME="/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home") 

sc <- spark_connect(master = "local")



model_builder_for_transport_price <- function(spark_conn, postgres_conn, path_to_model){
  # open data
  seed <- 12345678
  path_to_model <- paste(args[1], "/model_transport_price", sep="")
  peru = dbGetQuery(con, 'SELECT price_transport_net, mean_of_transport, country_of_arrival, custom, net_price, net_price_per_unit  
                        FROM peru_imp ') 
  peru_tbl <- sdf_copy_to(sc, peru, overwrite = T)
  transformation <- mutate(peru_tbl, price_transport_net = price_transport_net %>% as.numeric(),
                           net_price = net_price  %>% as.numeric(),
                           net_price_per_unit = net_price_per_unit %>% as.numeric(),
                           net_price_per_unit2 = net_price_per_unit^2,
                           price_transport = price_transport_net - net_price,
                           price_transport = case_when(price_transport < 0 ~ 0,
                                                       TRUE ~ price_transport),
                           price_transport_net=NULL,
                           net_price2 = net_price^2, 
                           net_price3 = net_price^3,
                           net_price4 = net_price^4,
                           net_price5 = net_price^5,
                           net_price6 = net_price^6,
                           net_price7 = net_price^7,
                           net_price8 = net_price^8,
                           net_price9 = net_price^9,
                           net_price10 = net_price^10,
                           net_price_per_unit2 = net_price_per_unit^2
  ) 

  transformation_pipeline <- ml_pipeline(sc) %>% ft_dplyr_transformer(tbl = transformation)
  partition <- peru_tbl %>% sdf_random_split(training = 0.9, test = 0.1, seed = seed) 
  
  models <- NULL
  models[[1]] <-  ml_fit_and_transform(transformation_pipeline, partition$'train') %>%
    ml_linear_regression(price_transport ~
                                        net_price + net_price_per_unit + net_price_per_unit2 +
                           country_of_arrival + mean_of_transport + custom)
  
  models[[2]]  <-ml_fit_and_transform(transformation_pipeline, partition$'train')  %>%
    ml_linear_regression(price_transport ~ 
                                         net_price + net_price2 +
                                         net_price_per_unit +
                                         mean_of_transport +
                                         country_of_arrival +
                                         custom)
  
  models[[3]]  <-ml_fit_and_transform(transformation_pipeline, partition$'train') %>%
    ml_linear_regression(price_transport ~
                                         net_price + net_price2 + net_price3 + 
                                         net_price4 + net_price5 + net_price6 +
                                         net_price7 + net_price8 + net_price9 +
                                         net_price10 +
                                         mean_of_transport +
                                         country_of_arrival +
                                         custom)
  
  
  # r2adj <- NULL
  # for(i in 1:length(models)){
  #   r2adj <- c(r2adj, models[[i]]$summary$r2adj)
  #   cat(paste("For model: ", models[[i]]$pipeline_model$stages[[1]]$formula,
  #               "\nThe R square adjusted index is: ", r2adj[[i]] %>% round(3), "\n\n"))
  # }
  
  # caching test (is not that large)
  test <- sdf_copy_to(sc,  ml_fit_and_transform(transformation_pipeline, partition$'test'), name="test", overwrite = T)
  
  # predictions
  rmse_test <- NULL
  for(i in 1:length(models)){
    # NOTE: rmse_test is collected. It is ok since it's at the end of the usage 
    # of spark. The models have already been applyed.
    rmse_test<- c(rmse_test,
                  (ml_predict(models[[i]], test) %>% mutate(prediction = 
                                                              case_when(prediction < 0 ~ 0, 
                                                                      TRUE ~ prediction)) %>%
                     mutate(square_diff = (price_transport-prediction)^2) %>% 
                     summarise(rmse = sqrt(mean(square_diff, na.rm = TRUE)))%>% collect %>% as.numeric) ) 
  }
  best_idx <- which.min(rmse_test)
  cat(paste("The best model in terms of root mean squares error is: ", 
              models[[best_idx]]$formula,
              "\nRMSE on the test = ", rmse_test[best_idx] %>% round(1),
              "\n\n"))
  
  final_pipeline <- transformation_pipeline %>%
    ft_r_formula(models[[best_idx]]$formula) %>%
    ml_linear_regression()
  partition1 <- peru_tbl %>% sdf_random_split(training = 0.9, test = 0.1, seed = seed) 
  
  fitted_pipeline <- ml_fit(final_pipeline, partition1$training)
  ml_save(fitted_pipeline, path_to_model)
  cat(" at ", path_to_model, "\n")
}

model_builder_for_transport_price(sc, con, args[1])


