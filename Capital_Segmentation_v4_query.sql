WITH Leads AS (
    SELECT DISTINCT salesforce_accountid
        , customer_id
        , origination_amount
        , origination_factor
        , probability_of_default
    FROM TOAST_CAPITAL_RISK_PROFILE
    INNER JOIN CUSTOMER_DIM USING(SALESFORCE_ACCOUNTID)
    WHERE FILTER_REASON = 'ELIGIBLE'
    AND is_latest = TRUE
    AND probability_of_default <= .20
    AND CC_repayment_rate <= .20
    AND MODEL_VERSION = 'model_v_2_4'
)

, most_recent_opp AS (
  SELECT row_number() OVER (PARTITION BY salesforce_accountid ORDER BY created_date DESC, IFNULL(funding_requested_date, '1970-01-01') DESC) ranks,
  salesforce_accountid, salesforce_toastcapitalopportunityid, origination_amount_currency, origination_factor_percent
  FROM toast_capital_opportunity_fact
)

SELECT DISTINCT
    customer.salesforce_accountid
    , customer.salesforce_parent_accountid
    , customer.customer_name
    , customer.restaurant_type
    , customer.RESTAURANT_TYPE_HIGH_LEVEL
    , customer.sales_division
    , customer.RS_tier
    , customer.city
    , CASE WHEN UPPER(customer.state) = 'WASHINGTON' THEN 'DC'
        WHEN UPPER(customer.state) = 'ILLINOIS' THEN 'IL'
        WHEN UPPER(customer.state) = 'INDIANA' THEN 'IN'
        WHEN UPPER(customer.state) = 'OHIO' THEN 'OH'
        WHEN UPPER(customer.state) = 'MASSACHUSETTS' THEN 'MA'
        WHEN UPPER(customer.state) IN ('VI', 'ON', 'AB', 'NULL', 'BB', 'GU', 'DR', 'MB', 'EX', 'NA') THEN 'Other'
        ELSE UPPER(customer.state) END AS state_master
    , CASE WHEN state_master IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'PA', 'NJ') THEN 'Northeast'
        WHEN state_master IN ('WI', 'MI', 'IL', 'IN', 'OH', 'NE', 'SD', 'ND', 'KS', 'MN', 'IA', 'MO') THEN 'Midwest'
        WHEN state_master IN ('DE', 'DC', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'MS', 'AL', 'OK', 'TX', 'AR', 'LA') THEN 'South'
        WHEN state_master IN ('ID', 'MT', 'WY', 'NV', 'UT', 'CO', 'AZ', 'NM', 'AK', 'WA', 'OR', 'CA', 'HI') THEN 'West'
        ELSE 'Other' END AS region
    , vol.payment_amount AS GMV
    , mod.LIVE_SAAS_MRR*12 AS live_saas_ARR
    , zeroifnull(parent.number_of_locations) as parent_number_of_locations
    , mod.live_api_module_count AS API_COUNT
    , mod.LIVE_CORE_SOFTWARE_MODULE_COUNT AS CORE_COUNT
    , mod.LIVE_ENTERPRISE_MODULE_COUNT AS ENTERPRISE_COUNT
    , mod.LIVE_OTHER_MODULE_COUNT AS OTHER_COUNT
    , mod.LIVE_KIOSK_MODULE_COUNT AS KIOSK_COUNT
    , mod.LIVE_LOYALTY_MODULE_COUNT AS LOYALTY_COUNT
    , mod.LIVE_HANDHELD_MODULE_COUNT AS HANDHELD_COUNT
    , mod.LIVE_GIFT_CARD_MODULE_COUNT AS GIFTCARD_COUNT
    , mod.LIVE_ONLINE_ORDERING_MODULE_COUNT AS OO_COUNT
    , mod.LIVE_INVENTORY_MODULE_COUNT AS INVENTORY_COUNT
    , mod.LIVE_KDS_MODULE_COUNT AS KDS_COUNT
    , nps.LATEST_NPS_SCORE
    , vm.OVERALL_VALUE_MAX_SCORE
    , FIRST_VALUE(tco.created_date) OVER(PARTITION BY customer.salesforce_accountid ORDER BY created_date ASC) AS min_created_date
    , CASE WHEN min_created_date > '2019-03-31' THEN true ELSE false END AS is_post_ramp
    , customer.first_order_date
    , DATEDIFF(DAY, customer.first_order_date, min_created_date) AS days_live_to_elig
-----defining leads based on opp created date
    , CASE WHEN min_created_date IS NOT NULL THEN true ELSE false END AS is_lead
    , funding_requested_date
    , MONTH(funding_requested_date) AS month_funding_requested
    , CASE WHEN MONTH(funding_requested_date) IN ('12.0', '1.0', '2.0') THEN 'Winter'
        WHEN MONTH(funding_requested_date) IN ('3.0', '4.0', '5.0') THEN 'Spring'
        WHEN MONTH(funding_requested_date) IN ('6.0', '7.0', '8.0') THEN 'Summer'
        WHEN MONTH(funding_requested_date) IN ('9.0', '10.0','11.0') THEN 'Fall'
        END AS season
    , CASE WHEN funding_requested_date IS NOT NULL THEN true ELSE false END AS is_MQL
    , final_offer_date
    , CASE WHEN final_offer_date IS NOT NULL THEN true ELSE false END AS is_opp
    , offer_signed_date
    , MONTH(offer_signed_date) AS month_offer_signed
    , CASE WHEN offer_signed_date IS NOT NULL THEN true ELSE false END AS is_deal
    , Leads.origination_amount
    , Leads.origination_factor
    , Leads.probability_of_default
    , tco.purpose_of_capital
    , tco.offer_declined_date
    , CASE WHEN offer_declined_date IS NOT NULL THEN TRUE ELSE FALSE END AS is_declined
FROM CUSTOMER_DIM customer
LEFT JOIN TOAST_CAPITAL_OPPORTUNITY_FACT tco ON customer.CUSTOMER_ID=tco.CUSTOMER_ID
LEFT JOIN most_recent_opp
    ON most_recent_opp.salesforce_accountid = tco.salesforce_accountid
    AND most_recent_opp.salesforce_toastcapitalopportunityid = tco.salesforce_toastcapitalopportunityid
    AND most_recent_opp.ranks = 1
INNER JOIN customer_dim as parent on parent.salesforce_accountid = customer.salesforce_parent_accountid
LEFT JOIN Leads ON tco.salesforce_accountid = Leads.salesforce_accountid
LEFT JOIN MONTHLY_CUSTOMER_LIVE_MODULES mod on mod.customer_id = customer.customer_id
LEFT JOIN CUSTOMER_VOLUME_LAST_MONTH_DIM vol on vol.customer_id = customer.customer_id
LEFT JOIN CUSTOMER_NPS_DIM nps ON nps.customer_id=customer.customer_id
LEFT JOIN VALUE_MAX_OVERALL vm ON vm.customer_id=customer.customer_id
WHERE
    customer.account_type = 'Customer' AND
    customer.churn_reason IS NULL AND
    customer.first_order_date < CURRENT_DATE()
    AND mod.YR_MO = '2020-01'
    AND vm.date_id = '20200101'
ORDER BY salesforce_accountid;
