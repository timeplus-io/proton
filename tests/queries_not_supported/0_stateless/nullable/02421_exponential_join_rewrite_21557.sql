-- Tags: long

-- https://github.com/ClickHouse/ClickHouse/issues/21557

DROP STREAM IF EXISTS store_returns;
DROP STREAM IF EXISTS catalog_sales;
DROP STREAM IF EXISTS catalog_returns;
DROP STREAM IF EXISTS date_dim;
DROP STREAM IF EXISTS store;
DROP STREAM IF EXISTS customer;
DROP STREAM IF EXISTS customer_demographics;
DROP STREAM IF EXISTS promotion;
DROP STREAM IF EXISTS household_demographics;
DROP STREAM IF EXISTS customer_address;
DROP STREAM IF EXISTS income_band;
DROP STREAM IF EXISTS item;

CREATE STREAM store_sales
(
    `ss_sold_date_sk` nullable(int64),
    `ss_sold_time_sk` nullable(int64),
    `ss_item_sk` int64,
    `ss_customer_sk` nullable(int64),
    `ss_cdemo_sk` nullable(int64),
    `ss_hdemo_sk` nullable(int64),
    `ss_addr_sk` nullable(int64),
    `ss_store_sk` nullable(int64),
    `ss_promo_sk` nullable(int64),
    `ss_ticket_number` int64,
    `ss_quantity` nullable(int64),
    `ss_wholesale_cost` nullable(float32),
    `ss_list_price` nullable(float32),
    `ss_sales_price` nullable(float32),
    `ss_ext_discount_amt` nullable(float32),
    `ss_ext_sales_price` nullable(float32),
    `ss_ext_wholesale_cost` nullable(float32),
    `ss_ext_list_price` nullable(float32),
    `ss_ext_tax` nullable(float32),
    `ss_coupon_amt` nullable(float32),
    `ss_net_paid` nullable(float32),
    `ss_net_paid_inc_tax` nullable(float32),
    `ss_net_profit` nullable(float32),
    `ss_promo_sk_nn` int16,
    `ss_promo_sk_n2` nullable(int16)
)
ENGINE = MergeTree ORDER BY (ss_item_sk, ss_ticket_number);

CREATE STREAM store_returns
(
    `sr_returned_date_sk` nullable(int64),
    `sr_return_time_sk` nullable(int64),
    `sr_item_sk` int64,
    `sr_customer_sk` nullable(int64),
    `sr_cdemo_sk` nullable(int64),
    `sr_hdemo_sk` nullable(int64),
    `sr_addr_sk` nullable(int64),
    `sr_store_sk` nullable(int64),
    `sr_reason_sk` nullable(int64),
    `sr_ticket_number` int64,
    `sr_return_quantity` nullable(int64),
    `sr_return_amt` nullable(float32),
    `sr_return_tax` nullable(float32),
    `sr_return_amt_inc_tax` nullable(float32),
    `sr_fee` nullable(float32),
    `sr_return_ship_cost` nullable(float32),
    `sr_refunded_cash` nullable(float32),
    `sr_reversed_charge` nullable(float32),
    `sr_store_credit` nullable(float32),
    `sr_net_loss` nullable(float32)
)
ENGINE = MergeTree ORDER BY (sr_item_sk, sr_ticket_number);

CREATE STREAM catalog_sales
(
    `cs_sold_date_sk` nullable(int64),
    `cs_sold_time_sk` nullable(int64),
    `cs_ship_date_sk` nullable(int64),
    `cs_bill_customer_sk` nullable(int64),
    `cs_bill_cdemo_sk` nullable(int64),
    `cs_bill_hdemo_sk` nullable(int64),
    `cs_bill_addr_sk` nullable(int64),
    `cs_ship_customer_sk` nullable(int64),
    `cs_ship_cdemo_sk` nullable(int64),
    `cs_ship_hdemo_sk` nullable(int64),
    `cs_ship_addr_sk` nullable(int64),
    `cs_call_center_sk` nullable(int64),
    `cs_catalog_page_sk` nullable(int64),
    `cs_ship_mode_sk` nullable(int64),
    `cs_warehouse_sk` nullable(int64),
    `cs_item_sk` int64,
    `cs_promo_sk` nullable(int64),
    `cs_order_number` int64,
    `cs_quantity` nullable(int64),
    `cs_wholesale_cost` nullable(float32),
    `cs_list_price` nullable(float32),
    `cs_sales_price` nullable(float32),
    `cs_ext_discount_amt` nullable(float32),
    `cs_ext_sales_price` nullable(float32),
    `cs_ext_wholesale_cost` nullable(float32),
    `cs_ext_list_price` nullable(float32),
    `cs_ext_tax` nullable(float32),
    `cs_coupon_amt` nullable(float32),
    `cs_ext_ship_cost` nullable(float32),
    `cs_net_paid` nullable(float32),
    `cs_net_paid_inc_tax` nullable(float32),
    `cs_net_paid_inc_ship` nullable(float32),
    `cs_net_paid_inc_ship_tax` nullable(float32),
    `cs_net_profit` nullable(float32)
)
ENGINE = MergeTree ORDER BY (cs_item_sk, cs_order_number);

CREATE STREAM catalog_returns
(
    `cr_returned_date_sk` nullable(int64),
    `cr_returned_time_sk` nullable(int64),
    `cr_item_sk` int64,
    `cr_refunded_customer_sk` nullable(int64),
    `cr_refunded_cdemo_sk` nullable(int64),
    `cr_refunded_hdemo_sk` nullable(int64),
    `cr_refunded_addr_sk` nullable(int64),
    `cr_returning_customer_sk` nullable(int64),
    `cr_returning_cdemo_sk` nullable(int64),
    `cr_returning_hdemo_sk` nullable(int64),
    `cr_returning_addr_sk` nullable(int64),
    `cr_call_center_sk` nullable(int64),
    `cr_catalog_page_sk` nullable(int64),
    `cr_ship_mode_sk` nullable(int64),
    `cr_warehouse_sk` nullable(int64),
    `cr_reason_sk` nullable(int64),
    `cr_order_number` int64,
    `cr_return_quantity` nullable(int64),
    `cr_return_amount` nullable(float32),
    `cr_return_tax` nullable(float32),
    `cr_return_amt_inc_tax` nullable(float32),
    `cr_fee` nullable(float32),
    `cr_return_ship_cost` nullable(float32),
    `cr_refunded_cash` nullable(float32),
    `cr_reversed_charge` nullable(float32),
    `cr_store_credit` nullable(float32),
    `cr_net_loss` nullable(float32)
)
ENGINE = MergeTree ORDER BY (cr_item_sk, cr_order_number);

CREATE STREAM date_dim
(
    `d_date_sk` int64,
    `d_date_id` string,
    `d_date` nullable(Date),
    `d_month_seq` nullable(int64),
    `d_week_seq` nullable(int64),
    `d_quarter_seq` nullable(int64),
    `d_year` nullable(int64),
    `d_dow` nullable(int64),
    `d_moy` nullable(int64),
    `d_dom` nullable(int64),
    `d_qoy` nullable(int64),
    `d_fy_year` nullable(int64),
    `d_fy_quarter_seq` nullable(int64),
    `d_fy_week_seq` nullable(int64),
    `d_day_name` nullable(string),
    `d_quarter_name` nullable(string),
    `d_holiday` nullable(string),
    `d_weekend` nullable(string),
    `d_following_holiday` nullable(string),
    `d_first_dom` nullable(int64),
    `d_last_dom` nullable(int64),
    `d_same_day_ly` nullable(int64),
    `d_same_day_lq` nullable(int64),
    `d_current_day` nullable(string),
    `d_current_week` nullable(string),
    `d_current_month` nullable(string),
    `d_current_quarter` nullable(string),
    `d_current_year` nullable(string)
)
ENGINE = MergeTree ORDER BY d_date_sk;

CREATE STREAM store
(
    `s_store_sk` int64,
    `s_store_id` string,
    `s_rec_start_date` nullable(Date),
    `s_rec_end_date` nullable(Date),
    `s_closed_date_sk` nullable(int64),
    `s_store_name` nullable(string),
    `s_number_employees` nullable(int64),
    `s_floor_space` nullable(int64),
    `s_hours` nullable(string),
    `s_manager` nullable(string),
    `s_market_id` nullable(int64),
    `s_geography_class` nullable(string),
    `s_market_desc` nullable(string),
    `s_market_manager` nullable(string),
    `s_division_id` nullable(int64),
    `s_division_name` nullable(string),
    `s_company_id` nullable(int64),
    `s_company_name` nullable(string),
    `s_street_number` nullable(string),
    `s_street_name` nullable(string),
    `s_street_type` nullable(string),
    `s_suite_number` nullable(string),
    `s_city` nullable(string),
    `s_county` nullable(string),
    `s_state` nullable(string),
    `s_zip` nullable(string),
    `s_country` nullable(string),
    `s_gmt_offset` nullable(float32),
    `s_tax_precentage` nullable(float32)
)
ENGINE = MergeTree ORDER BY s_store_sk;

CREATE STREAM customer
(
    `c_customer_sk` int64,
    `c_customer_id` string,
    `c_current_cdemo_sk` nullable(int64),
    `c_current_hdemo_sk` nullable(int64),
    `c_current_addr_sk` nullable(int64),
    `c_first_shipto_date_sk` nullable(int64),
    `c_first_sales_date_sk` nullable(int64),
    `c_salutation` nullable(string),
    `c_first_name` nullable(string),
    `c_last_name` nullable(string),
    `c_preferred_cust_flag` nullable(string),
    `c_birth_day` nullable(int64),
    `c_birth_month` nullable(int64),
    `c_birth_year` nullable(int64),
    `c_birth_country` nullable(string),
    `c_login` nullable(string),
    `c_email_address` nullable(string),
    `c_last_review_date` nullable(string)
)
ENGINE = MergeTree ORDER BY c_customer_sk;

CREATE STREAM customer_demographics
(
    `cd_demo_sk` int64,
    `cd_gender` nullable(string),
    `cd_marital_status` nullable(string),
    `cd_education_status` nullable(string),
    `cd_purchase_estimate` nullable(int64),
    `cd_credit_rating` nullable(string),
    `cd_dep_count` nullable(int64),
    `cd_dep_employed_count` nullable(int64),
    `cd_dep_college_count` nullable(int64)
)
ENGINE = MergeTree ORDER BY cd_demo_sk;

CREATE STREAM promotion
(
    `p_promo_sk` int64,
    `p_promo_id` string,
    `p_start_date_sk` nullable(int64),
    `p_end_date_sk` nullable(int64),
    `p_item_sk` nullable(int64),
    `p_cost` nullable(float64),
    `p_response_target` nullable(int64),
    `p_promo_name` nullable(string),
    `p_channel_dmail` nullable(string),
    `p_channel_email` nullable(string),
    `p_channel_catalog` nullable(string),
    `p_channel_tv` nullable(string),
    `p_channel_radio` nullable(string),
    `p_channel_press` nullable(string),
    `p_channel_event` nullable(string),
    `p_channel_demo` nullable(string),
    `p_channel_details` nullable(string),
    `p_purpose` nullable(string),
    `p_discount_active` nullable(string)
)
ENGINE = MergeTree ORDER BY p_promo_sk;

CREATE STREAM household_demographics
(
    `hd_demo_sk` int64,
    `hd_income_band_sk` nullable(int64),
    `hd_buy_potential` nullable(string),
    `hd_dep_count` nullable(int64),
    `hd_vehicle_count` nullable(int64)
)
ENGINE = MergeTree ORDER BY hd_demo_sk;

CREATE STREAM customer_address
(
    `ca_address_sk` int64,
    `ca_address_id` string,
    `ca_street_number` nullable(string),
    `ca_street_name` nullable(string),
    `ca_street_type` nullable(string),
    `ca_suite_number` nullable(string),
    `ca_city` nullable(string),
    `ca_county` nullable(string),
    `ca_state` nullable(string),
    `ca_zip` nullable(string),
    `ca_country` nullable(string),
    `ca_gmt_offset` nullable(float32),
    `ca_location_type` nullable(string)
)
ENGINE = MergeTree ORDER BY ca_address_sk;

CREATE STREAM income_band
(
    `ib_income_band_sk` int64,
    `ib_lower_bound` nullable(int64),
    `ib_upper_bound` nullable(int64)
)
ENGINE = MergeTree ORDER BY ib_income_band_sk;

CREATE STREAM item
(
    `i_item_sk` int64,
    `i_item_id` string,
    `i_rec_start_date` nullable(Date),
    `i_rec_end_date` nullable(Date),
    `i_item_desc` nullable(string),
    `i_current_price` nullable(float32),
    `i_wholesale_cost` nullable(float32),
    `i_brand_id` nullable(int64),
    `i_brand` nullable(string),
    `i_class_id` nullable(int64),
    `i_class` nullable(string),
    `i_category_id` nullable(int64),
    `i_category` nullable(string),
    `i_manufact_id` nullable(int64),
    `i_manufact` nullable(string),
    `i_size` nullable(string),
    `i_formulation` nullable(string),
    `i_color` nullable(string),
    `i_units` nullable(string),
    `i_container` nullable(string),
    `i_manager_id` nullable(int64),
    `i_product_name` nullable(string)
)
ENGINE = MergeTree ORDER BY i_item_sk;

EXPLAIN SYNTAX
WITH
    cs_ui AS
    (
        SELECT
            cs_item_sk,
            sum(cs_ext_list_price) AS sale,
            sum((cr_refunded_cash + cr_reversed_charge) + cr_store_credit) AS refund
        FROM catalog_sales , catalog_returns
        WHERE (cs_item_sk = cr_item_sk) AND (cs_order_number = cr_order_number)
        GROUP BY cs_item_sk
        HAVING sum(cs_ext_list_price) > (2 * sum((cr_refunded_cash + cr_reversed_charge) + cr_store_credit))
    ),
    cross_sales AS
    (
        SELECT
            i_product_name AS product_name,
            i_item_sk AS item_sk,
            s_store_name AS store_name,
            s_zip AS store_zip,
            ad1.ca_street_number AS b_street_number,
            ad1.ca_street_name AS b_street_name,
            ad1.ca_city AS b_city,
            ad1.ca_zip AS b_zip,
            ad2.ca_street_number AS c_street_number,
            ad2.ca_street_name AS c_street_name,
            ad2.ca_city AS c_city,
            ad2.ca_zip AS c_zip,
            d1.d_year AS syear,
            d2.d_year AS fsyear,
            d3.d_year AS s2year,
            count(*) AS cnt,
            sum(ss_wholesale_cost) AS s1,
            sum(ss_list_price) AS s2,
            sum(ss_coupon_amt) AS s3
        FROM store_sales
        , store_returns
        , cs_ui
        , date_dim AS d1
        , date_dim AS d2
        , date_dim AS d3
        , store
        , customer
        , customer_demographics AS cd1
        , customer_demographics AS cd2
        , promotion
        , household_demographics AS hd1
        , household_demographics AS hd2
        , customer_address AS ad1
        , customer_address AS ad2
        , income_band AS ib1
        , income_band AS ib2
        , item
        WHERE (ss_store_sk = s_store_sk) AND (ss_sold_date_sk = d1.d_date_sk) AND (ss_customer_sk = c_customer_sk) AND (ss_cdemo_sk = cd1.cd_demo_sk) AND (ss_hdemo_sk = hd1.hd_demo_sk) AND (ss_addr_sk = ad1.ca_address_sk) AND (ss_item_sk = i_item_sk) AND (ss_item_sk = sr_item_sk) AND (ss_ticket_number = sr_ticket_number) AND (ss_item_sk = cs_ui.cs_item_sk) AND (c_current_cdemo_sk = cd2.cd_demo_sk) AND (c_current_hdemo_sk = hd2.hd_demo_sk) AND (c_current_addr_sk = ad2.ca_address_sk) AND (c_first_sales_date_sk = d2.d_date_sk) AND (c_first_shipto_date_sk = d3.d_date_sk) AND (ss_promo_sk = p_promo_sk) AND (hd1.hd_income_band_sk = ib1.ib_income_band_sk) AND (hd2.hd_income_band_sk = ib2.ib_income_band_sk) AND (cd1.cd_marital_status != cd2.cd_marital_status) AND (i_color IN ('maroon', 'burnished', 'dim', 'steel', 'navajo', 'chocolate')) AND ((i_current_price >= 35) AND (i_current_price <= (35 + 10))) AND ((i_current_price >= (35 + 1)) AND (i_current_price <= (35 + 15)))
        GROUP BY
            i_product_name,
            i_item_sk,
            s_store_name,
            s_zip,
            ad1.ca_street_number,
            ad1.ca_street_name,
            ad1.ca_city,
            ad1.ca_zip,
            ad2.ca_street_number,
            ad2.ca_street_name,
            ad2.ca_city,
            ad2.ca_zip,
            d1.d_year,
            d2.d_year,
            d3.d_year
    )
SELECT
    cs1.product_name,
    cs1.store_name,
    cs1.store_zip,
    cs1.b_street_number,
    cs1.b_street_name,
    cs1.b_city,
    cs1.b_zip,
    cs1.c_street_number,
    cs1.c_street_name,
    cs1.c_city,
    cs1.c_zip,
    cs1.syear,
    cs1.cnt,
    cs1.s1 AS s11,
    cs1.s2 AS s21,
    cs1.s3 AS s31,
    cs2.s1 AS s12,
    cs2.s2 AS s22,
    cs2.s3 AS s32,
    cs2.syear,
    cs2.cnt
FROM cross_sales AS cs1 , cross_sales AS cs2
WHERE (cs1.item_sk = cs2.item_sk) AND (cs1.syear = 2000) AND (cs2.syear = (2000 + 1)) AND (cs2.cnt <= cs1.cnt) AND (cs1.store_name = cs2.store_name) AND (cs1.store_zip = cs2.store_zip)
ORDER BY
    cs1.product_name ASC,
    cs1.store_name ASC,
    cs2.cnt ASC,
    cs1.s1 ASC,
    cs2.s1 ASC
FORMAT Null
;

SELECT 'Ok';

DROP STREAM IF EXISTS store_returns;
DROP STREAM IF EXISTS catalog_sales;
DROP STREAM IF EXISTS catalog_returns;
DROP STREAM IF EXISTS date_dim;
DROP STREAM IF EXISTS store;
DROP STREAM IF EXISTS customer;
DROP STREAM IF EXISTS customer_demographics;
DROP STREAM IF EXISTS promotion;
DROP STREAM IF EXISTS household_demographics;
DROP STREAM IF EXISTS customer_address;
DROP STREAM IF EXISTS income_band;
DROP STREAM IF EXISTS item;
