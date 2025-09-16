### Instructions to Upload Word2Vec Model to git 

The zip files in this folder are not committed to git. Instead when you have a new word2vec model, please use the below instruction to publish it to nexus

First make a jar file from the zip

```
zip h2o-jobsnskills-word2vec.jar h2o-jobsnskills-word2vec.zip
mvn deploy:deploy-file -DgroupId=com.adp.datacloud.ds -DartifactId=h2o-jobsnskills-word2vec -Dversion=1.0 -Dpackaging=jar -Dfile=h2o-jobsnskills-word2vec.jar -DrepositoryId=releases -Durl=http://cdladpinexus01:8081/nexus/content/repositories/releases/ 
```