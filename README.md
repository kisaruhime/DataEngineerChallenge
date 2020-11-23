# DataEngineerChallenge

This is an interview challenge for PayPay. Please feel free to fork. Pull Requests will be ignored.

The challenge is to make make analytical observations about the data using the distributed tools below.

## Processing & Analytical goals:

1. Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a session.
    https://en.wikipedia.org/wiki/Session_(web_analytics)

2. Determine the average session time

3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.

4. Find the most engaged users, ie the IPs with the longest session times

## Additional questions for Machine Learning Engineer (MLE) candidates:
1. Predict the expected load (requests/second) in the next minute

2. Predict the session length for a given IP

3. Predict the number of unique URL visits by a given IP

## Tools allowed (in no particular order):
- Spark (any language, but prefer Scala or Java)
- Pig
- MapReduce (Hadoop 2.x only)
- Flink
- Cascading, Cascalog, or Scalding

If you need Hadoop, we suggest 
HDP Sandbox:
http://hortonworks.com/hdp/downloads/
or 
CDH QuickStart VM:
http://www.cloudera.com/content/cloudera/en/downloads.html


### Additional notes:
- You are allowed to use whatever libraries/parsers/solutions you can find provided you can explain the functions you are implementing in detail.
- IP addresses do not guarantee distinct users, but this is the limitation of the data. As a bonus, consider what additional data would help make better analytical conclusions
- For this dataset, complete the sessionization by time window rather than navigation. Feel free to determine the best session window time on your own, or start with 15 minutes.
- The log file was taken from an AWS Elastic Load Balancer:
http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format



## How to complete this challenge:

1. Fork this repo in github
2. Complete the processing and analytics as defined first to the best of your ability with the time provided.
3. Place notes in your code to help with clarity where appropriate. Make it readable enough to present to the PayPay interview team.
4. Include the test code and data in your solution. 
5. Complete your work in your own github repo and send the results to us and/or present them during your interview.

## What are we looking for? What does this prove?

We want to see how you handle:
- New technologies and frameworks
- Messy (ie real) data
- Understanding data transformation
This is not a pass or fail test, we want to hear about your challenges and your successes with this particular problem.

---
## Useful notes regarding challenge:
1. I decided at first to 'look' at provided data and to identify possible issues. 
For that case, i used jupyter notebook and pandas lib, it's really 
straightforward and quick-to-use functionality.
2. As a result of the previous point I came up with a decision to preprocess data, specifically:
    - to change the file format to csv, cause it's much handier
    - to sort data depend on the time column, it's necessary for sessionization
    - to remove columns, which will not take part in further transformations for sure
3. Corresponding  Jupyter notebook you can find in **jnotebook** folder
4. Concise python script for preprocessing data you can find in **pyscript** folder
   Prerequisites:
    - python 3.8.6
    - pandas 1.1.4
5. To run corresponding python script just run following command from console:

    `python pyscript\preprocess_data.py filepath_to_initail_data filepath_to_edited_data`
6. For processing data by sessions i used spark structured streaming and then for 
achieving analytical goals from challenge used spark SQL and sql itself.

7. I've run spark job locally, inside IDE that i am using - Intellij IDEA with 
preconfigured installed spark on mu workstation(just simple local installation)
To run spark from cmd :

    `mvn clean install`
    
    `spark-submit  target\DataEngineerChallenge-1.0-SNAPSHOT-spring-boot.jar csv\`