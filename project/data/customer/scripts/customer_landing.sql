CREATE EXTERNAL TABLE stedi-db-bj.customer_landing (
                             	  `customername` string,
                             	  `email` string,
                             	  `phone` string,
                             	  `birthday` string,
                             	  `serialnumber` string,
                             	  `registrationdate` bigint,
                             	  `lastupdatedate` bigint,
                             	  `sharewithresearchasofdate` bigint,
                             	  `sharewithpublicasofdate` bigint,
                             	  `sharewithfriendsasofdate` bigint
                             	)
                             	STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
                             	OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                             	LOCATION 's3://${{ secrets.MY_BUCKET }}/customer/'
                             	TBLPROPERTIES ('CreatedByJob'='landing', 'classification'='json', 'CreatedByJobRun'='jr_447ed258ab0c1d2cfdd56d5618a0353b6465cb30be2cc75b80d83c77081516ec');