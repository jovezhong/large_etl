cd large_etl/
spark-shell --master local[*] --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 -i etl.scala
