setwd("~/Desktop/BDM/Project/Data.nosync/Peru/CSV/EXP")
library(tidyverse)
d <- read_delim(file.choose(), delim=",", col_names= F)
header <- read.table(file.choose(), sep=";")
colnames(d) <- header
View(d)
a
nrow(d)
nrow(d %>% distinct())
ncol(d)
d %>% select(NUMEROITEM)
