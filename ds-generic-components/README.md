# DataScience Generic Components for EMR Pipelines

## Overview
  Contains reusable spark programs which can be integrated to other projects.

## Maintainers
  * Siddhardha Sagar Chinne - siddarthasagar.chinne@adp.com
  * Pavan Kumar Soma - pavan.soma@adp.com
  * Vishal Nara - vishal.nara@adp.com
  * Vamshikrishna Gunnala - vamshikrishna.gunnala@adp.com
  * Manoj Oleti - manoj.oleti@adp.com

## Components
  * <b>spark-cube : </b> A configuration driven framework for general purpose cubing using Spark.
  * <b>spark-feature-engineering : </b> Reusable spark code for feature engineering at scale.
  * <b>spark-connect-by : </b> Generic component to compute reporting hierarchies, layers and quite a few other metrics.  
  * <b>rdbms-data-extractor : </b> Reusable data extractor utility to run free-form SQL queries against databases and retrieve results into HDFS/S3.
  * <b>spark-insight-engine: </b> A configuration driven framework for mining insights based on statistical significance
  * <b>spark-data-sync: </b> A configuration driven framework to enable data-pull-down between clusters.
  * <b>spark-shape-monitor: </b> A reusable framework for computing dataset shapes and identifying outliers in them.

## shadow-libs
  * on databricks run time, some of provided versions for open source libraries avialabe in the class path override the versions pagacked inside the our component far jar and create unanticipated issues. to avoid it we have created ower own version of the shadowed jar though shadow-jar project

  ```bash
  cd shadow-jar
  ./gradlew clean assemble
  cd -
  ``` 

  - Note: the above command dumps the shadow jar to shadow-libs which is added as a file dependecy to any project needed. This is a one time job and only needs to be run if these a change required. the final jars in the shadow-libs directory needs to be checked into git on modification. 

## Developer Environment setup

  * Navigate to **spark-ds-parent** folder at command line
  * Run the following command to generate the eclipse project (and classpath) files for all projects in this repo.
 
  		./gradlew clean assemble
  		
  * Open Eclipse IDE. Import the projects into workspace.
  * Ensure that Scala compile compliance level is set to Fixed 2.10.x installation in Eclipse project preferences. This has to be done for each project being imported into eclipse
  * Run scala files in test folders simply using "Run as scala application".
    
## Command sequences

Refer to the READMEs of individual components.

## Documentation
  * [Confluence](To be updated)