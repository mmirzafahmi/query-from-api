# Analyzing Google Analytics Dataset Using Bigquery


All analytics reports are mostly useless unless if you don't know how to make sense of the numbers and then turn them into actionable insights. Google Analytics is a web analytics service that tracks and reports website traffic. 
<br/>
<br/>
## Count total sessions
To determine the total sessions for all of users, we could count the *session_id* by concatenating the *fullVisitorId* and *visitStartTime*, then count the total of *session_id* 
```postgresql
SELECT 
    COUNT(
        CONCAT(
            fullVisitorId, CAST(visitStartTime AS STRING)
        )
    ) AS total_sessions
FROM 
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
WHERE visits = 1;
```
or you can count *visits* which is the number of sessions according to [Bigquery GA Export Schema](https://support.google.com/analytics/answer/3437719?hl=en)
```postgresql
SELECT 
    SUM(visits) AS total_session
FROM 
    `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`;
```
<br/>

## Total sessions for each visitor
```postgresql
SELECT 
    fullVisitorId AS visitor, 
    COUNT(
        CONCAT(
            fullVisitorId, CAST(visitStartTime AS STRING)
        )
    ) AS total_sessions
FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export`
WHERE visits = 1
GROUP BY 1;
```
<br/>

## Average time to reach order_confirmation per session
```postgresql
SELECT
    fullVisitorId,
    session_number,
    AVG(IF(isInteraction IS NOT NULL AND eventCategory LIKE '%order_confirmation',
        hit_time, 0)) OVER (PARTITION BY session_number ORDER BY fullVisitorId, session_number) AS time_to_reach_confirm_in_minutes
FROM (
    SELECT
        fullVisitorId,
        hits.isInteraction,
        hits.eventCategory,
        ROW_NUMBER() OVER (PARTITION BY fullVisitorId, visitStartTime) AS session_number,
        hits.time / 1000 / 60 AS hit_time
    FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` AS GA, UNNEST(GA.hit) AS hits
)
WHERE isInteraction IS NOT NULL
ORDER BY 1,2
;
```

## Analyze how often user tend to change their location versus in checkout and on order placement. Demonstrate the deviation and check whether users who change address ended placing orders and delivered succesfully.
```postgresql
WITH t1 AS (
    SELECT 
        *,
        CASE 
            WHEN IFNULL(LEAD(distance) OVER (PARTITION BY fullVisitorId, hitNumber ORDER BY fullVisitorId, hitNumber), 0) = distance THEN 0
            ELSE 1
        END AS isChLoc
    FROM (
        SELECT
            fullVisitorId,
            hitNumber,
            country,
            locationCity,
            locationLon,
            locationLat,
            transactionId,
            orderPaymentMethod,
            eventAction,
            eventCategory,
            landingScreenName,
            c1,
            c2,
            IFNULL(ROUND(ST_DISTANCE(ST_GEOGPOINT(locationLat, locationLon), LEAD(ST_GEOGPOINT(locationLat, locationLon)) OVER (PARTITION BY fullVisitorId, hitNumber ORDER BY fullVisitorId, hitNumber)), 2), 0) AS distance
        FROM (
            SELECT
                fullVisitorId,
                country,
                hits.transactionId,
                hits.hitNumber,
                hits.eventAction,
                hits.eventCategory,
                hits.screenName,
                hits.eventLabel,
                hits.landingScreenName,
                CASE 
                    WHEN eventAction = 'transaction.attempted' AND CONTAINS_SUBSTR(eventLabel, 'adyen') THEN 'Adyen'
                    WHEN eventAction = 'transaction.attempted' AND CONTAINS_SUBSTR(eventLabel, 'paypal') THEN 'PayPal'
                    WHEN eventAction = 'transaction.attempted' AND CONTAINS_SUBSTR(eventLabel, 'googlepay') THEN 'GooglePay'
                    WHEN eventAction = 'transaction.attempted' AND CONTAINS_SUBSTR(eventLabel, 'cod') THEN 'COD'
                    WHEN eventAction = 'transaction.attempted' THEN 'Others'
                    ELSE 'Unknown'
                END AS orderPaymentMethod,
                CASE WHEN eventCategory LIKE '%checkout' OR eventCategory LIKE '%order_confirmation' THEN hitNumber END AS c1,
                CASE
                    WHEN hitNumber < MIN(
                        CASE 
                            WHEN eventCategory LIKE '%checkout' OR eventCategory LIKE '%order_confirmation' 
                            THEN hitNumber 
                        END
                    ) OVER (PARTITION BY fullVisitorId) THEN hitNumber 
                END AS c2,
                MAX(IF(hits.eventCategory LIKE '%checkout',1,0)) OVER (PARTITION BY fullVisitorId) AS isCheckout,
                MAX(IF(hits.eventCategory LIKE '%order_confirmation',1,0)) OVER (PARTITION BY fullVisitorId) AS isConfirmPurchased,
                --(SELECT value FROM UNNEST(hits.customDimensions) WHERE index=11) AS event,
                (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=16) AS locationCity,
                SAFE_CAST((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=19) AS FLOAT64) AS locationLon,
                SAFE_CAST((SELECT value FROM UNNEST(hits.customDimensions) WHERE index=18) AS FLOAT64) AS locationLat
            FROM `dhh-analytics-hiringspace.GoogleAnalyticsSample.ga_sessions_export` AS ga, 
            UNNEST(hit) AS hits
            ORDER BY 1,2
        )
        WHERE locationLon IS NOT NULL AND locationLat IS NOT NULL
        ORDER BY 1
    )
)
SELECT 
    fullVisitorId,
    country,
    locationCity,
    locationLon,
    locationLat,
    landingScreenName,
    orderPaymentMethod,
    MAX(CASE WHEN eventAction = 'login.succeeded' THEN 1 ELSE 0 END) AS userLoggedIn,
    MAX(CASE 
        WHEN eventAction IN ('address_update.clicked', 'Change Location', 'other_location.clicked', 'address_update.submitted') THEN 1
        ELSE 0
    END) AS isChangeLoc,
    MAX(CASE WHEN eventAction = 'transaction' THEN 1 ELSE 0 END) AS isPlacingOrder,
    MAX(CASE WHEN td.geopointDropoff IS NOT NULL THEN 1 ELSE 0 END) AS isDelivered,
    STDDEV(CASE WHEN hitNumber < (SELECT MIN(c1) FROM t1 AS b WHERE b.fullVisitorId = a.fullVisitorId) THEN distance ELSE 0 END) AS dev_before,
    STDDEV(CASE WHEN hitNumber > (SELECT MIN(c1) FROM t1 AS b WHERE b.fullVisitorId = a.fullVisitorId) THEN distance ELSE 0 END) AS dev_after,
    SUM(CASE WHEN hitNumber < (SELECT MIN(c1) FROM t1 AS b WHERE b.fullVisitorId = a.fullVisitorId) THEN isChLoc ELSE 0 END) AS change_loc_before_checkout,
    SUM(CASE WHEN hitNumber > (SELECT MIN(c1) FROM t1 AS b WHERE b.fullVisitorId = a.fullVisitorId) THEN isChLoc ELSE 0 END) AS change_loc_after_checkout
FROM t1 AS a
LEFT JOIN (
    SELECT frontendOrderId, geopointDropoff
    FROM `dhh-analytics-hiringspace.BackendDataSample.transactionalData` 
) AS td ON td.frontendOrderId=a.transactionId 
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1;
```

## Dashboard
To view the visualization, check [this link](https://datastudio.google.com/s/lJwx423sZ8U).