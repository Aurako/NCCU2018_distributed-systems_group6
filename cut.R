#用法：commandline : Rscript cut.R 檔案名.csv

args=commandArgs(trailingOnly =TRUE)
data <- read.csv(args[1])
datalen=nrow(data)
print(datalen)

if (datalen>5000000){
  num=as.integer(datalen/5000000)
    for (i in c(1:num+2)){
      name=paste0("datapart",i,".csv")
      print(name)
      finish<-i*5000000
      print(finish)
      start<-1+5000000*(i-1)
      print(start)
      datapi<-data[start:finish,]
      print(datapi)
      write.csv(datapi, file = name)
    } 
}
