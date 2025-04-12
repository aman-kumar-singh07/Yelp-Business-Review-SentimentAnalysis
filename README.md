# Yelp-Business-Review-SentimentAnalysis

Tech stack being used here Amazon S3, Python, Snowflake and SQL. We will first split the large JSON file (5GB- 7 million) into smaller files using Python for faster data ingestion. Then we will upload that data in s3 and then Snowflake. Lastly we will answer some interesting business problems using SQL.

## 📌 Problem 1: Find the number of businesses in each category

<pre> with cte as ( select business_id, trim(A.value) as category from tbl_yelp_businesses,
  lateral split_to_table(categories, ',') A ) 
  select category, count(*) as no_of_business from cte
  group by 1 
  order by 2 desc; </pre>

📌 Problem 2: Find the top 10 users who have reviewed the most businesses in the “Restaurants” category
<pre> select r.user_id, count(distinct r.business_id)
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b on r.business_id = b.business_id
where b.categories ilike '%restaurant%'
group by 1
order by 2 desc
limit 10;</pre>

📌 Problem 3: Find the most popular categories of businesses (based on the number of reviews).
<pre> with cte as (
    select business_id, trim(A.value) as category
    from tbl_yelp_businesses
    ,lateral split_to_table(categories,',') A
)
select category, count(*) as no_of_reviews
from cte
inner join tbl_yelp_reviews r on cte.business_id = r.business_id
group by 1
order by 2 desc; </pre>

📌 Problem 4: Find the top 3 most recent reviews for each business.
<pre> with cte as (
    select r.*, b.name,
           row_number() over(partition by r.business_id order by review_date desc) as rn
    from tbl_yelp_reviews r
    inner join tbl_yelp_businesses b on r.business_id = b.business_id
)
select * from cte
where rn <= 3; </pre>
📌 Problem 5: Find the month with the highest number of reviews.
<pre>select month(review_date) as review_month, count(*) as no_of_reviews
from tbl_yelp_reviews
group by 1
order by 2 desc; </pre>
📌 Problem 6: Find the percentage of 5-star reviews for each business.
<pre> select 
    b.business_id,
    b.name, 
    count(*) as total_reviews,
    sum(case when r.review_stars = 5 then 1 else 0 end) as five_star_reviews,
    (sum(case when r.review_stars = 5 then 1 else 0 end) * 100.0 / count(*)) as five_star_percentage
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b 
    on r.business_id = b.business_id
group by 1,2; </pre>
📌 Problem 7: Find the top 5 most reviewed businesses in each city.
<pre> with cte as (
    select 
        b.city, 
        b.business_id, 
        b.name, 
        count(*) as total_reviews
    from tbl_yelp_reviews r
    inner join tbl_yelp_businesses b 
        on r.business_id = b.business_id
    group by 1,2,3
) 
select * 
from cte 
qualify row_number() over (partition by city order by total_reviews desc) <= 5; </pre>
📌 Problem 8: Find the average rating of businesses that have at least 100 reviews.
<pre> select 
    b.business_id, 
    b.name, 
    count(*) as total_reviews, 
    avg(review_stars) as avg_rating
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b 
    on r.business_id = b.business_id
group by 1,2
having count(*) >= 100; </pre>

📌 Problem 9: List the top 10 users who have written the most reviews, along with the businesses they reviewed.
<pre> with cte as (
    select r.user_id, count(*) as total_reviews
    from tbl_yelp_reviews r
    inner join tbl_yelp_businesses b on r.business_id = b.business_id
    group by 1
    order by 2 desc
    limit 10
)

select user_id, business_id
from tbl_yelp_reviews
where user_id in (select user_id from cte)
group by 1, 2
order by user_id; </pre>
📌 Problem 10: Find the top 10 businesses with the highest positive sentiment reviews.
<pre> select r.business_id, b.name, count(*) as total_reviews
from tbl_yelp_reviews r
inner join tbl_yelp_businesses b on r.business_id = b.business_id
where sentiments = 'Positive'
group by 1, 2
order by 3 desc
limit 10;</pre>
