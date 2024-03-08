use data_bank;
select * from customer_nodes;
select * from customer_transactions;
select * from regions;

select count(*) from customer_nodes;
select count(*) from customer_transactions;

select * from customer_transactions order by customer_id;
select * from customer_nodes order by customer_id;



-- A. Customer Nodes Exploration
-- 1. How many unique nodes are there on the Data Bank system?

select distinct node_id from customer_nodes;
select count(distinct node_id) from customer_nodes;

-- 2. What is the number of nodes per region?

select r.region_name, count(cn.node_id) as number_of_nodes_per_region
from regions as r
join customer_nodes as cn
on r.region_id = cn.region_id
group by r.region_name
order by number_of_nodes_per_region DESC;
-- Australia has the highest number of nodes.

-- 3. How many customers are allocated to each region?

select r.region_name,count(distinct cn.customer_id) as total_customers
from regions as r
join customer_nodes as cn
on r.region_id = cn.region_id
group by r.region_name
order by total_customers DESC;
-- The highest number of customers are allocated to australia region

-- 4. How many days on average are customers reallocated to a different node?
select count(*) from customer_nodes where end_date like '%9999%';

SELECT DISTINCT YEAR(start_date) FROM customer_nodes;
SELECT DISTINCT YEAR(end_date) FROM customer_nodes;

select * from customer_nodes;
select * from customer_transactions;
select * from regions;

SELECT AVG(DATEDIFF(end_date, Start_date)) as avg_day
FROM customer_nodes
WHERE end_date != '9999-12-31';
-- customers are reallocted to a different region on average of 14 days.

-- 5. What is the median, 80th and 95th percentile for this same reallocation 
--  days metric for each region?
select * from customer_nodes;
select * from customer_transactions;
select * from regions;

with date_diff as 
(select cn.customer_id, cn.region_id, r.region_name,
datediff(end_date,start_date) as reallocation_days
from customer_nodes as cn
inner join regions as r
on cn.region_id = r.region_id
where end_date != '9999-12-31')

select distinct region_id, region_name, 
percentile_cont(0.5) within group(order by reallocation_days) over(partition by region_name) as median,
percentile_cont(0.8) within group(order by reallocation_days) over(partition by region_name) as percentile_80,
percentile_cont(0.95) within group(order by reallocation_days) over(partition by region_name) as percentile_95
from date_diff
order by region_name;

-- All regions have the same median and 95th percentile.
               
-- B. Customer Transactions
-- 1. What is the unique count and total amount for each transaction type?

use data_bank;

select txn_type, Count(*) as unique_count, sum(txn_amount) as Total_amount 
from customer_transactions
group by txn_type
order by txn_type;
-- deposits are more than purchases then withdrawals

-- 2. What is the average total historical deposit counts and amounts for all
-- customers?

select customer_id, count(txn_type) from customer_transactions where txn_type = 'deposit'
group by customer_id;

with Summary as (select customer_id, txn_type, count(*) as type_count, sum(txn_amount) as type_amount
from customer_transactions
group by customer_id, txn_type)

select txn_type, avg(type_count) as avg_deposit_count, avg(type_amount) as avg_deposit_amount
from summary
where txn_type = 'deposit'
group by txn_type;

-- avg deposit count for a customer is 5 and 
-- avg deposit amount for a customer is 2948


-- 3. For each month - how many Data Bank customers make more than 1
-- deposit and either 1 purchase or 1 withdrawal in a single month?
select customer_id, month(txn_date) as month_num,
monthname(txn_date) as month_name,
count(case when txn_type = 'deposit' then 1 end) as deposit_count,
count(case when txn_type= 'purchase' then 1 end) as purchase_count,
count(case when txn_type= 'withdrawal' then 1 end) as withdrawal_count
from customer_transactions
group by customer_id, month_num, month_name;


with type_txn_count_by_month as
(select customer_id, month(txn_date) as month_num,
monthname(txn_date) as month_name,
count(case when txn_type = 'deposit' then 1 end) as deposit_count,
count(case when txn_type= 'purchase' then 1 end) as purchase_count,
count(case when txn_type= 'withdrawal' then 1 end) as withdrawal_count
from customer_transactions
group by customer_id, month_num, month_name)

select month_num, month_name, count(distinct customer_id) as customer_count
from type_txn_count_by_month
where deposit_count > 1 and (purchase_count > 0 OR withdrawal_count > 0)
group by month_num, month_name;

-- March  has the highest # of customers 204 when have made more than 1 .......
-- April has the least



-- 4. What is the closing balance for each customer at the end of the month?

select * from customer_transactions
order by txn_date;


with cte as ( select customer_id, DATE_FORMAT(txn_date, '%Y-%m-01') AS month_start,
sum(case when txn_type = 'deposit' then txn_amount else -1 * txn_amount end) as total_amount
from customer_transactions 
group by customer_id, month_start)

select customer_id, month(month_start) as month,
monthname(month_start) as month_name,
sum(total_amount) over (partition by customer_id order by month_start) as closing_balance
from cte;


-- 5. What is the percentage of customers who increase their closing balance by more than 5%?
select * from customer_nodes;
select * from customer_transactions;
select * from regions;
-- extra
select customer_id, LAST_DAY(txn_date) AS end_date,
sum(case when txn_type in ('withdrawal','purchase') then -txn_amount
else txn_amount end) as transactions
from customer_transactions
group by customer_id,end_date;

-- answer last try
-- monthly transaction of each customer
with 
monthly_transactions as 
(select customer_id, LAST_DAY(txn_date) AS end_date,
sum(case when txn_type in ('withdrawal','purchase') then -txn_amount
else txn_amount end) as transactions
from customer_transactions
group by customer_id,end_date),

-- closing balance for each customer for each month
closing_balances as
(select customer_id, end_date, coalesce(sum(transactions) over (partition by customer_id order by 
end_date rows between unbounded preceding and current row),0) as closing_balance
from monthly_transactions),

-- % increase in closing balance for each customer for each month
percent_increase as
(select customer_id, end_date, closing_balance,
lag(closing_balance) over (partition by customer_id order by end_date) as prev_closing_balance,
100 * (closing_balance - lag(closing_balance) over (partition by customer_id order by end_date))/
nullif(lag(closing_balance) over(partition by customer_id order by end_date),0) as p_increase
from closing_balances)

-- % of customers whose closing balance increased 5% compared to pre month
select cast(100 * count(distinct customer_id) / (select count(distinct customer_id) from customer_transactions
) as float) as p_customers
from percent_increase
where p_increase > 5 ;
-- 70% customers closing balance  has increased by 5% compared to pre month.


-- C. Data Allocation Challenge
-- To test out a few different hypotheses - the Data Bank team wants to run an experiment where 
-- different groups of customers would be allocated data using 3 different options:
-- ● Option 1: data is allocated based off the amount of money at the end of the previous month
-- ● Option 2: data is allocated on the average amount of money kept in the account in the previous
--  30 days
-- ● Option 3: data is updated real-time

-- For this multi-part challenge question - you have been requested to generate
-- the following data elements to help the Data Bank team estimate how much
-- data will need to be provisioned for each option:

-- 1) ● running customer balance column that includes the impact each transaction
-- running balance for each customer based on the order of their transaction.

SELECT customer_id, txn_date, txn_type, txn_amount, 
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type = 'withdrawal' THEN -txn_amount
WHEN txn_type = 'purchase' THEN -txn_amount
ELSE 0 END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions;

-- 2) ● customer balance at the end of each month
-- closing balance for each customer for each month

SELECT customer_id, month(txn_date) AS month, 
monthname(txn_date) AS month_name,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type = 'withdrawal' THEN -txn_amount
WHEN txn_type = 'purchase' THEN -txn_amount
ELSE 0 END) AS closing_balance
FROM customer_transactions
GROUP BY customer_id, month, month_name
order by customer_id;


-- 3)● minimum, average and maximum values of the running balance for each
-- customer

WITH run_balance AS
(SELECT customer_id, txn_date, txn_type, txn_amount,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
WHEN txn_type = 'withdrawal' THEN -txn_amount
WHEN txn_type = 'purchase' THEN -txn_amount
ELSE 0 END) OVER(PARTITION BY customer_id ORDER BY txn_date) AS running_balance
FROM customer_transactions)

SELECT customer_id, AVG(running_balance) AS avg_running_balance,
MIN(running_balance) AS min_running_balance,
MAX(running_balance) AS max_running_balance
FROM run_balance
GROUP BY customer_id;

-- Using all of the data available - how much data would have been required for
-- each option on a monthly basis?
-- option 1
WITH transaction_amt_cte AS
(SELECT customer_id, txn_date, MONTH(txn_date) AS txn_month, txn_type, 
CASE WHEN txn_type = 'deposit' THEN txn_amount 
ELSE -txn_amount END AS net_transaction_amt
FROM customer_transactions),

running_customer_balance_cte AS
(SELECT customer_id, txn_date, txn_month, net_transaction_amt,
SUM(net_transaction_amt) OVER(PARTITION BY customer_id, txn_month ORDER BY txn_date
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_customer_balance
FROM transaction_amt_cte),

customer_end_month_balance_cte AS
(SELECT customer_id, txn_month, MAX(running_customer_balance) AS month_end_balance
FROM running_customer_balance_cte
GROUP BY customer_id, txn_month)

SELECT txn_month,
SUM(month_end_balance) AS data_required_per_month
FROM customer_end_month_balance_cte
GROUP BY txn_month
ORDER BY data_required_per_month DESC;
-- INSIGHTS:-
-- january requires more monthly data allocation then march then feb
-- april requires the least
-- this means data allocation required varies with months
-- customers tend to do have higher end month balances in January and March than in 
-- Frebruary and April so more data should be should be allocated for January, followed 
-- by March, February and April.

-- option 2
WITH transaction_amt_cte AS
(SELECT customer_id, MONTH(txn_date) AS txn_month,
SUM(CASE WHEN txn_type = 'deposit' THEN txn_amount
ELSE -txn_amount END) AS net_transaction_amt
FROM customer_transactions
GROUP BY customer_id, MONTH(txn_date)),

running_customer_balance_cte AS
(SELECT customer_id, txn_month, net_transaction_amt,
SUM(net_transaction_amt) OVER(PARTITION BY customer_id ORDER BY txn_month) AS running_customer_balance
FROM transaction_amt_cte),

avg_running_customer_balance AS
(SELECT customer_id,
AVG(running_customer_balance) AS avg_running_customer_balance
FROM running_customer_balance_cte
GROUP BY customer_id)

SELECT txn_month, ROUND(SUM(avg_running_customer_balance), 0) AS data_required_per_month
FROM running_customer_balance_cte r
JOIN avg_running_customer_balance a
ON r.customer_id = a.customer_id
GROUP BY txn_month
ORDER BY data_required_per_month;

-- INSIGHTS
-- Based on our query output, the average running customer balance is negative for all four months,
-- indicating that customers tend to withdraw more money than they deposit on average.

-- The data required for February and March are higher than for January and April, 
-- suggesting that more data should be allocated for those two months.


-- option 3

WITH transaction_amt_cte AS (SELECT customer_id, txn_date, 
MONTH(txn_date) AS txn_month, -- Use proper assignment here
txn_type, txn_amount,
CASE WHEN txn_type = 'deposit' THEN txn_amount 
ELSE -txn_amount 
END AS net_transaction_amt -- Adjusted alias
FROM 
customer_transactions),

running_customer_balance_cte AS 
(SELECT customer_id, txn_month,
SUM(net_transaction_amt) OVER (PARTITION BY customer_id ORDER BY txn_month) AS running_customer_balance
FROM transaction_amt_cte)

SELECT txn_month, SUM(running_customer_balance) AS data_required_per_month
FROM running_customer_balance_cte
GROUP BY txn_month
ORDER BY data_required_per_month;

-- The data required for the month of March is significantly higher than for the other months.
-- This shows that there were more transactions happening in March than in the other months.



-- D. Extra Challenge
-- Data Bank wants to try another option which is a bit more difficult to
-- implement - they want to calculate data growth using an interest calculation,
-- just like in a traditional savings account you might have with a bank.
-- If the annual interest rate is set at 6% and the Data Bank team wants to reward
-- its customers by increasing their data allocation based off the interest
-- calculated on a daily basis at the end of each day, how much data would be
-- required for this option on a monthly basis?
-- Special notes:
-- ● Data Bank wants an initial calculation which does not allow for
-- compounding interest, however they may also be interested in a daily
-- compounding interest calculation so you can try to perform this
-- calculation if you have the stamina!

WITH cte AS (SELECT 
customer_id, txn_date, 
SUM(txn_amount) AS total_data,
STR_TO_DATE(CONCAT(YEAR(txn_date), '-', MONTH(txn_date), '-', '01'), '%Y-%m-%d') AS month_start_date,
DATEDIFF(txn_date, LAST_DAY(txn_date) - INTERVAL DAY(LAST_DAY(txn_date)) DAY) + 1 AS days_in_month,
CAST(SUM(txn_amount) AS DECIMAL(18, 2)) * POW((1 + 0.06/365), DATEDIFF(txn_date, '1900-01-01')) AS daily_interest_data
FROM customer_transactions
GROUP BY customer_id, txn_date)

SELECT customer_id,
DATE_FORMAT(month_start_date, '%Y-%m-%d') AS txn_month,
ROUND(SUM(daily_interest_data * days_in_month), 2) AS data_required
FROM cte
GROUP BY customer_id, DATE_FORMAT(month_start_date, '%Y-%m-%d')
ORDER BY data_required DESC;

