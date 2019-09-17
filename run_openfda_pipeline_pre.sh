ulimit -c unlimited
java -Dspark.master=local[*] -Dspark.sql.broadcastTimeout=60000 -Dspark.executor.heartbeatInterval=60000 -Dspark.sql.crossJoin.enabled=true -Dspark.driver.maxResultSize=0 -cp metorikku-standalone.jar com.yotpo.metorikku.Metorikku -c openfda_pre.yaml
