/*
#######################################################################################

Task	  :			ETL Analytics
Date      :			08/04/2024 
Desc      :			ETL To Migrate and Model New Table from 6 Data Source Tables
Purpose   :			Provide an ETL that updates a table and ensure updated data always
Author    :			Clement Ekeocha

########################################################################################
*/

-- ETL Process

-- Step 1: Create the unified table by joining the relevant tables

CREATE TABLE unified_table AS
SELECT
    c.admin_id,
    c.country_code,
    c.name AS company_name,
    c.created_at AS company_created_at,
    u.user_id,
    u.full_name AS user_name,
    u.created_at AS user_created_at,
    co.country_name,
    co.region,
    o.order_id,
    o.created_at AS order_created_at,
    o.revenue AS order_revenue
FROM company c
JOIN users u ON c.admin_id = u.admin_id
JOIN countries co ON c.country_code = co.country_code
JOIN orders o ON u.user_id = o.user_id;

/* 
This unified table contains all the key data we need to answer the questions, 
including company information, user information, country information, and order details.

*/

-- Questions

-- (1) Daily/Weekly/Monthly Dynamic of Revenue and Orders per Country, Region:


-- Revenue and orders per country per day
SELECT 
    DATE(order_created_at) AS order_date,
    country_name,
    region,
    SUM(order_revenue) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM unified_table
GROUP BY order_date, country_name, region
ORDER BY order_date, total_revenue DESC;



-- Revenue and orders per country per week
SELECT 
    DATE_TRUNC('week', order_created_at) AS order_week,
    country_name,
    region,
    SUM(order_revenue) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM unified_table 
GROUP BY order_week, country_name, region
ORDER BY order_week, total_revenue DESC;



-- Revenue and orders per country per month
SELECT 
    DATE_TRUNC('month', order_created_at) AS order_month,
    country_name,
    region,
    SUM(order_revenue) AS total_revenue,
    COUNT(order_id) AS total_orders
FROM unified_table
GROUP BY order_month, country_name, region
ORDER BY order_month, total_revenue DESC;
--------------------------------------------------------------------------------------------------------------------------------------------------

-- (2) Top 10 companies by number of orders in the last week:

SELECT 
    c.name AS company_name,
    COUNT(order_id) AS total_orders
FROM unified_table
WHERE order_created_at >= DATE_SUB(CURDATE(), INTERVAL 1 WEEK)
GROUP BY c.name
ORDER BY total_orders DESC
LIMIT 10;
-------------------------------------------------------------------------------------------------------------------------------------------------

-- (3) Number of companies signed up:

SELECT COUNT(DISTINCT admin_id) AS total_companies 
FROM unified_table;

-------------------------------------------------------------------------------------------------------------------------------------------------

-- (4) Number of monthly active users (at least with 1 completed order):

SELECT COUNT(DISTINCT user_id) AS monthly_active_users
FROM unified_table
WHERE order_created_at >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH);

-------------------------------------------------------------------------------------------------------------------------------------------------


/*
incremental load approach
*/

-- Create a stored procedure for the incremental load
DELIMITER $$
CREATE PROCEDURE load_incremental_unified_table()
BEGIN
    -- Create a temporary table to hold the new data
    CREATE TEMPORARY TABLE new_unified_data AS
    SELECT
        c.admin_id,
        c.country_code,
        c.name AS company_name,
        c.created_at AS company_created_at,
        u.user_id,
        u.full_name AS user_name,
        u.created_at AS user_created_at,
        co.country_name,
        co.region,
        o.order_id,
        o.created_at AS order_created_at,
        o.revenue AS order_revenue
    FROM company c
    JOIN users u ON c.admin_id = u.admin_id
    JOIN countries co ON c.country_code = co.country_code
    JOIN orders o ON u.user_id = o.user_id
    WHERE o.created_at >= (SELECT MAX(order_created_at) FROM UNIFIED_TABLE);

    -- Insert the new data into the UNIFIED_TABLE
    INSERT INTO UNIFIED_TABLE
    SELECT * FROM new_unified_data;

    -- Drop the temporary table
    DROP TABLE new_unified_data;
END $$
DELIMITER ;


-------------------------------------------------------------------------------------------------------------------------------------------------

-- Create a scheduled event to run the load_incremental_unified_table() procedure daily at 11:00 PM
CREATE EVENT daily_load_incremental_unified_table
ON SCHEDULE EVERY 1 DAY
STARTS '2024-04-08 23:59:00'
DO CALL load_incremental_unified_table();

