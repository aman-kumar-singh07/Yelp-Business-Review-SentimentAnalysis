# Yelp-Business-Review-SentimentAnalysis

Tech stack being used here Python, Snowflake and SQL. We will first split the large JSON file into smaller files using Python for faster data ingestion. Then we will upload that data in s3 and then Snowflake. Lastly we will answer some interesting business problems using SQL.

## 📌 Problem 1: Find the number of businesses in each category

<pre> with cte as ( select business_id, trim(A.value) as category from tbl_yelp_businesses,
  lateral split_to_table(categories, ',') A ) 
  select category, count(*) as no_of_business from cte
  group by 1 
  order by 2 desc; </pre>
