WITH search_combine AS (
    SELECT *
    FROM `momo-ads-data.momowa.ecapp_search_v2`
),
-- 搜索資料中的商品資料，取得關鍵字、商品編號、頁數、member_id、點擊與曝光等資訊
search_date_search_cnt AS (
    SELECT DISTINCT 
        JSON_VALUE(goodsList, '$.searchKeyword') AS searchKeyword, 
        JSON_VALUE(goodsList, '$.goodsCode') AS goodsCode, 
        JSON_VALUE(goodsList, '$.searchId') AS searchId,  
        CAST(JSON_VALUE(goodsList, '$.page') AS INT64) AS page,  
        member_id,
        CAST(json_value(goodsList, '$.visibilityRate') AS INT64) AS visibilityRate,
        CASE WHEN act_name = "impression" THEN 1 ELSE 0 END AS impression,
        CASE WHEN act_name = "click" THEN 1 ELSE 0 END AS click,
        create_time,
        CAST(JSON_VALUE(goodsList, '$.position') AS FLOAT64) AS position
    FROM search_combine t,
         UNNEST(JSON_QUERY_ARRAY(event_attrs, '$.goodsList')) AS goodsList
    -- 檢查條件：排除非商品搜尋、非 MT01 搜尋、非曝光商品、無搜尋 ID、廣告商品
    WHERE DATE(t.create_time) >='2025-07-20' and DATE(t.create_time) <= '2025-07-26'
      AND CAST(JSON_VALUE(goodsList, '$.position') AS FLOAT64) IS NOT NULL  
      AND JSON_VALUE(goodsList, '$.searchType') = 'MT01'
      AND t.event_type = 'searchResultGoodsList'
      AND JSON_VALUE(goodsList, '$.adAttrs.adRequestId') = ''
),
latest_date AS (
  select
    MIN(DATE(create_time)) AS start_date,
    MAX(DATE(create_time)) AS end_date
  FROM search_date_search_cnt
),
--------------------------關鍵字維度_搜索次數&人數-----------------------
-- 每個關鍵字每日的搜索次數（searchId，僅計算 page=1）
search_keyword_count AS (
    SELECT 
        -- DATE(create_time) as create_date,  --若要更新每日資料可保留
        searchKeyword,
        COUNT(DISTINCT searchId) AS total_search_count
    FROM search_date_search_cnt
    WHERE page = 1
    GROUP BY 1
),
-- 每個關鍵字每日的搜尋人數（member_id）
unique_search_count AS (
    SELECT
        -- DATE(create_time) as create_date,
        searchKeyword,
        COUNT(DISTINCT member_id) AS unique_search_count
    FROM search_date_search_cnt
    GROUP BY 1
),
-- 搜索總結(搜索次數與人數合併)
keyword_final AS (
    SELECT
        -- skc.create_date,
        skc.searchKeyword,
        skc.total_search_count,
        usc.unique_search_count
    FROM search_keyword_count skc
    JOIN unique_search_count usc
    ON skc.searchKeyword = usc.searchKeyword
    -- AND skc.create_date = usc.create_date
),
--------------二級品類維度_曝光商品數&曝光點擊---------------
-- 每筆搜尋記錄對應的商品、曝光與點擊狀態、可視率與1P/3P標記
keyword_goods AS (
    SELECT 
        create_time,        
        searchKeyword,
        goodsCode,
        visibilityRate,
        click,
        impression,
        (page-1)*20+position as position,
        CASE WHEN LEFT(goodsCode,2) = 'TP' THEN '3P' ELSE '1P' END AS party
    FROM search_date_search_cnt 
),
-- 加上品類欄位：商品所屬的一級與二級品類
goods_layer_g AS (
    SELECT 
        -- DATE(kg.create_time) as create_date,
        kg.searchKeyword,
        kg.goodsCode,
        gd.ENTP_CODE as entp_code,
        gd.L1_CAT_NAME AS layer1,
        gd.L2_CAT_NAME AS layer2,
        kg.party
    FROM keyword_goods kg
    LEFT JOIN `momo-ads-data.shared_data.ALL_GOODS_INFO_DAILY_1P3P` gd
    ON kg.goodsCode = gd.GOODS_CODE
),
-- 每個關鍵字品類下曝光商品總數
goods_count AS (
    SELECT 
        -- create_date,
        searchKeyword,
        layer1,
        layer2,
        COUNT(DISTINCT CASE WHEN party = '1P' THEN goodsCode END) AS goods_cnt_1P,
        COUNT(DISTINCT CASE WHEN party = '3P' THEN goodsCode END) AS goods_cnt_3P,
        COUNT(DISTINCT goodsCode) AS total_goods_cnt,
        COUNT(DISTINCT CASE WHEN party = '1P' THEN entp_code END) AS entp_cnt_1P,
        COUNT(DISTINCT CASE WHEN party = '3P' THEN entp_code END) AS entp_cnt_3P,
        COUNT(DISTINCT entp_code) AS total_entp_cnt,        
    FROM goods_layer_g   
    GROUP BY 1,2,3
),
-- 每個商品的曝光與點擊數（拆分為1P與3P），加上可視率的條件
goods_layer AS (
    SELECT
        -- DATE(kg.create_time) as create_date,
        kg.searchKeyword,
        COALESCE(kg.visibilityRate, 0) AS visibilityRate,
        gd.L1_CAT_NAME AS layer1,
        gd.L2_CAT_NAME AS layer2,
        CASE WHEN party = '1P' THEN COALESCE(click, 0) ELSE 0 END AS clicks_1P,
        CASE WHEN party = '3P' THEN COALESCE(click, 0) ELSE 0 END AS clicks_3P,
        CASE WHEN party = '1P' THEN COALESCE(impression, 0) ELSE 0 END AS impressions_1P,
        CASE WHEN party = '3P' THEN COALESCE(impression, 0) ELSE 0 END AS impressions_3P,
        position,
        CASE WHEN party = '1P' THEN COALESCE(position, 0) ELSE 0 END AS position_1P,
        CASE WHEN party = '3P' THEN COALESCE(position, 0) ELSE 0 END AS position_3P        
    FROM keyword_goods kg
    LEFT JOIN `momo-ads-data.shared_data.ALL_GOODS_INFO_DAILY_1P3P` gd
    ON kg.goodsCode = gd.GOODS_CODE
),
-- 彙總可視率 >= 80 的商品曝光與點擊數
click_impression AS (
    SELECT 
        -- create_date,
        searchKeyword,
        layer1,
        layer2,
        SUM(impressions_1P) AS impression_1P,
        SUM(impressions_3P) AS impression_3P,
        SUM(clicks_1P) AS click_1P,
        SUM(clicks_3P) AS click_3P     
    FROM goods_layer
    WHERE COALESCE(visibilityRate, 0) >= 80
    GROUP BY 1,2,3
),
position AS (
    SELECT
        -- create_date,
        searchKeyword,
        layer1,
        layer2,
        avg(position) as avg_position,
        avg(position_1P) as avg_position_1P,
        avg(position_3P) as avg_position_3P 
    FROM goods_layer
    GROUP BY 1,2,3  
),
-- 每個商品對應的曝光商品數、品類資訊
goods_norecord AS (
    SELECT 
        -- gl.create_date,
        gl.searchKeyword,
        gl.layer1,
        gl.layer2,
        gl.goodsCode,
        gl.party,
        ci.impression_1P,
        ci.impression_3P,
        ci.click_1P,
        ci.click_3P,
        gc.goods_cnt_1P,
        gc.goods_cnt_3P,
        gc.total_goods_cnt,
        p.avg_position,
        p.avg_position_1P,
        p.avg_position_3P,
        gc.entp_cnt_1P,
        gc.entp_cnt_3P,
        gc.total_entp_cnt
    FROM goods_layer_g gl
    LEFT JOIN click_impression ci
    ON gl.searchKeyword = ci.searchKeyword
    AND gl.layer1 = ci.layer1
    AND gl.layer2 = ci.layer2
    -- AND gl.create_date = ci.create_date
    LEFT JOIN position p
    ON gl.searchKeyword = p.searchKeyword
    AND gl.layer1 = p.layer1
    AND gl.layer2 = p.layer2
    -- AND gl.create_date = p.create_date    
    LEFT JOIN goods_count gc
    ON gl.searchKeyword = gc.searchKeyword
    AND gl.layer1 = gc.layer1
    AND gl.layer2 = gc.layer2
    -- AND gl.create_date = gc.create_date
),
--------------------------搜索歸因的業績--------------------------
-- 搜索事件與訂單資料做連結，並加上品類資訊
sdc_party AS (
    SELECT 
        create_time,        
        searchKeyword,
        L1_CAT_NAME AS layer1,
        L2_CAT_NAME AS layer2,
        member_id,
        searchId,
        goodsCode,
        visibilityRate,
        click,
        impression,
        CASE 
            WHEN LEFT(goodsCode,2) = 'TP' THEN '3P'
            ELSE '1P'
        END AS party
    FROM search_date_search_cnt sdc
    LEFT JOIN `momo-ads-data.shared_data.ALL_GOODS_INFO_DAILY_1P3P` gd
    ON sdc.goodsCode = gd.GOODS_CODE
),
-- 訂單資料，並標註每筆訂單是 1P 或 3P
ord AS (
    SELECT 
        *,
        CASE 
            WHEN LEFT(GOODS_CODE,2) = 'TP' THEN '3P'
            ELSE '1P'
        END AS party
    FROM `momo-ads-data.shared_data.ORDERDAIL_1P3P`
),
-- 搜索資料與訂單資料做 Join，設定 24 小時內轉換歸因機制
wa_ord AS (
    SELECT
        sdc_p.create_time,
        sdc_p.searchKeyword,
        sdc_p.layer1,
        sdc_p.layer2,
        sdc_p.goodsCode,
        sdc_p.member_id,
        sdc_p.party,
        sdc_p.searchId,
        sdc_p.click,
        ORDER_DATE,
        ORDER_AMT,
        CASE WHEN ord.party = '1P' THEN ord.ENTP_CODE END AS entp_1P,
        CASE WHEN ord.party = '3P' THEN ord.ENTP_CODE END AS entp_3P,
        COALESCE(CASE WHEN ord.party = '1P' THEN ord.ORDER_NO END, '') AS ORDER_NO_1P,
        COALESCE(CASE WHEN ord.party = '3P' THEN ord.ORDER_NO END, '') AS ORDER_NO_3P,
        COALESCE(CASE WHEN ord.party = '1P' THEN ord.ORDER_AMT END, 0) AS ORDER_AMT_1P,
        COALESCE(CASE WHEN ord.party = '3P' THEN ord.ORDER_AMT END, 0) AS ORDER_AMT_3P,
        COALESCE(CASE WHEN ord.party = '1P' THEN ord.ORDER_QTY END, 0) AS ORDER_QTY_1P,
        COALESCE(CASE WHEN ord.party = '3P' THEN ord.ORDER_QTY END, 0) AS ORDER_QTY_3P,
        TIMESTAMP_DIFF(ORDER_DATE, TIMESTAMP(create_time), HOUR) AS hours_diff,
        -- click_rank_desc：該會員對該商品的最後一次點擊
        RANK() OVER(PARTITION BY member_Id, goodsCode ORDER BY create_time DESC) AS click_rank_desc,
        -- order_rank_asc：該會員對該商品的最早一筆訂單
        RANK() OVER(PARTITION BY CUST_NO, GOODS_CODE ORDER BY ORDER_DATE ASC) AS order_rank_asc
    FROM sdc_party sdc_p
    LEFT JOIN ord 
    ON sdc_p.member_id = ord.CUST_NO 
    AND sdc_p.goodsCode = ord.GOODS_CODE
    AND sdc_p.party = ord.party
    AND TIMESTAMP_DIFF(ORDER_DATE, TIMESTAMP(create_time), HOUR) BETWEEN 0 AND 24
    WHERE sdc_p.click > 0 --點擊轉換的訂單
),
-- 將訂單資料與 click_rank/order_rank 條件套入，過濾為符合歸因規則的轉換資料
org_conversion as (
  SELECT
    create_time,
    searchKeyword,
    layer1,
    layer2,
    --concat(searchId,goodsCode) as click_no,
    CASE WHEN click_rank_desc = 1 THEN ORDER_NO_1P ELSE NULL END AS order_no_1P,#只取 last click 對到的訂單
    CASE WHEN click_rank_desc = 1 THEN ORDER_NO_3P ELSE NULL END AS order_no_3P,
    CASE WHEN click_rank_desc = 1 THEN ORDER_AMT_1P ELSE NULL END AS order_amt_1P, #只取 last click 對到的訂單金額
    CASE WHEN click_rank_desc = 1 THEN ORDER_AMT_3P ELSE NULL END AS order_amt_3P,
    CASE WHEN click_rank_desc = 1 THEN ORDER_QTY_1P ELSE NULL END AS order_qty_1P, 
    CASE WHEN click_rank_desc = 1 THEN ORDER_QTY_3P ELSE NULL END AS order_qty_3P, 
    ORDER_DATE,
    hours_diff,
    click_rank_desc,
    order_rank_asc
   FROM wa_ord
  LEFT JOIN `momo-ads-data.shared_data.ALL_GOODS_INFO_DAILY_1P3P` AS gd
  ON wa_ord.goodsCode =  gd.GOODS_CODE
  WHERE order_rank_asc = 1 # 有造成轉換的click只抓最早的訂單 # 或是沒有轉換的click
),
-- 每個關鍵字在各品類下的訂單匯總（以金額與筆數統計）
org_record AS (
    SELECT
        -- DATE(create_time) AS create_date,
        searchKeyword,
        layer1,
        layer2,
        SUM(order_amt_1P) AS org_order_amt_1P,
        SUM(order_amt_3P) AS org_order_amt_3P,
        SUM(order_qty_1P) AS org_order_qty_1P,
        SUM(order_qty_3P) AS org_order_qty_3P,
        COUNT(DISTINCT order_no_1P) AS org_order_cnt_1P,
        COUNT(DISTINCT order_no_3P) AS org_order_cnt_3P
    FROM org_conversion
    GROUP BY 1,2,3
),
-- 商品為單位的轉換歸因（只取 order_amt）
org_conversion_g AS (
    SELECT
        create_time,
        searchKeyword,
        layer1,
        layer2,
        goodsCode,
        party,
        CASE WHEN click_rank_desc = 1 THEN ORDER_AMT ELSE NULL END AS order_amt,
        ORDER_DATE,
        hours_diff,
        click_rank_desc,
        order_rank_asc
    FROM wa_ord
    LEFT JOIN `momo-ads-data.shared_data.ALL_GOODS_INFO_DAILY_1P3P` gd
    ON wa_ord.goodsCode = gd.GOODS_CODE
    WHERE order_rank_asc = 1
),
-- 每個商品是否有銷售（以商品為單位的統計）
org_record_g AS (
    SELECT
        -- DATE(create_time) AS create_date,
        searchKeyword,
        layer1,
        layer2,
        party,
        goodsCode,
        SUM(order_amt) AS org_order_amt
    FROM org_conversion_g
    GROUP BY 1,2,3,4,5
),
-- 各關鍵字、品類下售出的商品數量（以預約金額是否大於 0 為標準）
sold_goods AS (
    SELECT 
        -- create_date,
        searchKeyword,
        layer1,
        layer2,
        COUNT(DISTINCT CASE WHEN party = '1P' AND org_order_amt > 0 THEN goodsCode END) AS sold_goods_1P,
        COUNT(DISTINCT CASE WHEN party = '3P' AND org_order_amt > 0 THEN goodsCode END) AS sold_goods_3P
    FROM org_record_g 
    GROUP BY 1,2,3
),
-- 整合所有關鍵字 x 品類 x 日期的彙總資料，包含搜尋、曝光、點擊、銷售等指標
final_result AS (
    SELECT 
        -- gn.create_date AS search_date,  -- 計算指標的日期
        DENSE_RANK() OVER (
            -- PARTITION BY gn.create_date 
            ORDER BY kf.total_search_count DESC, kf.unique_search_count DESC
        ) AS search_rank,  -- 以關鍵字的搜尋次數排序
        kf.searchKeyword,
        kf.total_search_count AS search_count,
        kf.unique_search_count AS search_users,
        c.department,
        gn.layer1,
        gn.layer2,
        gn.goods_cnt_1P AS product_1P,
        gn.goods_cnt_3P AS product_3P,
        gn.total_goods_cnt AS product_total,
        gn.avg_position,
        gn.avg_position_1P,
        gn.avg_position_3P,
        gn.impression_1P, 
        gn.impression_3P,
        gn.click_1P,           
        gn.click_3P,
        s.sold_goods_1P,
        s.sold_goods_3P,
        r.org_order_amt_1P AS order_amt_1P,     -- 最後點擊歸因的訂單金額&數量
        r.org_order_amt_3P AS order_amt_3P,
        r.org_order_qty_1P AS order_qty_1P,
        r.org_order_qty_3P AS order_qty_3P,
        r.org_order_cnt_1P as order_cnt_1P,
        r.org_order_cnt_3P as order_cnt_3P,       
        entp_cnt_1P,
        entp_cnt_3P,
        total_entp_cnt
    FROM goods_norecord gn
    LEFT JOIN org_record r
        ON gn.searchKeyword = r.searchKeyword
        AND gn.layer1 = r.layer1
        AND gn.layer2 = r.layer2
        -- AND gn.create_date = r.create_date
    LEFT JOIN sold_goods s
        ON gn.searchKeyword = s.searchKeyword
        AND gn.layer1 = s.layer1
        AND gn.layer2 = s.layer2
        -- AND gn.create_date = s.create_date
    LEFT JOIN keyword_final kf
        ON gn.searchKeyword = kf.searchKeyword
        -- AND gn.create_date = kf.create_date
    LEFT JOIN `momo-ads-data.daniel_test.二級品類_1P3P部門對照` c --將品類的1P.3P部門歸在一起
        ON gn.layer1 = c.layer1
        AND gn.layer2 = c.layer2
    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28
),
-- 保留品類曝光商品數佔比高的關鍵字
-- layer_ratio_base AS (
--   SELECT *,
--     SUM(product_total) OVER (PARTITION BY searchKeyword) AS keyword_product_total,
--     SAFE_DIVIDE(product_total, SUM(product_total) OVER (PARTITION BY searchKeyword)) AS layer_ratio_in_keyword
--   FROM final_result
-- ),
-- layer_core_keyword_filtered AS (
--   SELECT *
--   FROM (  
--     SELECT *,
--       COUNT(DISTINCT CONCAT(layer1, layer2)) OVER (PARTITION BY searchKeyword) AS n_layer,
--       RANK() OVER (PARTITION BY searchKeyword ORDER BY layer_ratio_in_keyword DESC) AS top_layer_rank
--     FROM layer_ratio_base
--   )
--   WHERE 
--     (n_layer = 1 OR layer_ratio_in_keyword >= 0.1 OR top_layer_rank = 1)
-- ),
-- 加上關鍵字類型：是否為品牌字
keyword_type AS (
    SELECT *,
        searchKeyword AS keyword_,
        CASE 
            WHEN bd_kw.keyword IS NOT NULL THEN '品牌字'
            ELSE '品類字'
        END AS keyword_category
    FROM final_result f
    LEFT JOIN (
        SELECT DISTINCT LOWER(TRIM(searchkeyword)) AS keyword
        FROM `momo-ads-data.Paul_dataset.top_brand_extension_with_brand_no_v2` --此資料為廣告組人工維護，並非收錄所有品牌字
    ) bd_kw
    ON LOWER(TRIM(f.searchKeyword)) = bd_kw.keyword
)
-- 最終輸出：兩週內每個關鍵字下二級品類所對應的各項指標彙總
SELECT
    FORMAT_DATE('%Y/%m/%d', DATE(start_date)) || '～' || FORMAT_DATE('%Y/%m/%d', DATE(end_date)) AS date_range,
    search_rank,
    keyword_ AS keyword,
    keyword_category,
    search_count,
    search_users,
    department,
    layer1,
    layer2,
    product_total,
    product_1P,
    product_3P,
    total_entp_cnt,  
    entp_cnt_1P,
    entp_cnt_3P,
    avg_position,
    avg_position_1P,
    avg_position_3P,
    (impression_1P+impression_3P) as impression_total,
    impression_1P,
    impression_3P,
    (click_1P+click_3P) as click_total,
    click_1P,
    click_3P,
    IFNULL(sold_goods_1P, 0) + IFNULL(sold_goods_3P, 0) AS sold_goods_total,
    IFNULL(sold_goods_1P, 0) AS sold_goods_1P,
    IFNULL(sold_goods_3P, 0) AS sold_goods_3P,
    IFNULL(order_amt_1P, 0) + IFNULL(order_amt_3P, 0) AS order_amt_total,
    IFNULL(order_amt_1P, 0) AS order_amt_1P,
    IFNULL(order_amt_3P, 0) AS order_amt_3P,
    IFNULL(order_qty_1P, 0) + IFNULL(order_qty_3P, 0) AS order_qty_total,
    IFNULL(order_qty_1P, 0) AS order_qty_1P,
    IFNULL(order_qty_3P, 0) AS order_qty_3P,
    (CASE 
        WHEN IFNULL(order_qty_1P, 0) = 0 THEN 0 
        ELSE IFNULL(order_cnt_1P, 0) 
     END + 
     CASE 
        WHEN IFNULL(order_qty_3P, 0) = 0 THEN 0 
        ELSE IFNULL(order_cnt_3P, 0) 
     END) AS order_cnt_total,
    CASE 
        WHEN IFNULL(order_qty_1P, 0) = 0 THEN 0 
        ELSE IFNULL(order_cnt_1P, 0) 
    END AS order_cnt_1P,
    CASE 
        WHEN IFNULL(order_qty_3P, 0) = 0 THEN 0 
        ELSE IFNULL(order_cnt_3P, 0) 
    END AS order_cnt_3P
FROM keyword_type
CROSS JOIN latest_date ld
WHERE layer1 IS NOT NULL 
AND layer2 IS NOT NULL  -- 過濾沒有品類對應的資料
AND keyword_ IS NOT NULL -- 過濾沒有關鍵字的資料
AND search_users > 5 * (DATE_DIFF(end_date, start_date, DAY) + 1)
ORDER BY search_rank ASC;

############### R
---
title: "Untitled"
output: html_document
date: "2025-07-15"
---
```{r}
library(dplyr)
library(readxl)
library(openxlsx)
library(readr)
library(reactable)
library(htmlwidgets)
library(scales)
library(tidyr)
library(stringr)

kw <- read_csv("C:/Users/Lily Peng/Downloads/0820-0821_kw.csv")
colnames(kw)

# 若 department 欄位存在 NA，則顯示警告並停止執行
if (any(is.na(kw$department))) {
  warning("欄位 department 存在 NA 值，請先檢查資料")
  kw_na <- kw %>% filter(is.na(department))
  print(kw_na)
  stop("程式已終止")
}


# kw_na_1 = kw_na %>% select(layer1,layer2,department) %>% distinct 
# 
# write.xlsx(kw_na_1, "test.xlsx", overwrite = TRUE)
```

```{r}
df_kw <- data.frame(kw) 

df_kw <- df_kw %>%
  mutate(across(
    .cols = c(
      rank = search_rank,                              
      search_count, 
      search_users, 
      product_total,
      product_1P,
      product_3P,
      avg_position,
      avg_position_1P,
      avg_position_3P,
      impression_1P,
      impression_3P,
      impression_total,
      click_1P,
      click_3P,
      click_total,
      sold_goods_1P,
      sold_goods_3P,
      sold_goods_total,
      order_amt_1P,
      order_amt_3P,
      order_amt_total,
      order_qty_1P,
      order_qty_3P,
      order_qty_total,
      order_cnt_1P,
      order_cnt_3P,
      order_cnt_total,
      entp_cnt_1P,
      entp_cnt_3P,
      total_entp_cnt
    ),
    .fns = ~ as.numeric(.)
  )) %>%
  mutate(
    # CTR: click / impression
    CTR_1P = ifelse(impression_1P > 0, click_1P / impression_1P, NA_real_),
    CTR_3P = ifelse(impression_3P > 0, click_3P / impression_3P, NA_real_),
    CTR_total = ifelse(impression_total > 0, click_total / impression_total, NA_real_),

    # CVR: order_cnt / click
    轉單率_1P = ifelse(click_1P > 0, order_cnt_1P / click_1P, NA_real_),
    轉單率_3P = ifelse(click_3P > 0, order_cnt_3P / click_3P, NA_real_),
    轉單率_total = ifelse(click_total > 0, order_cnt_total / click_total, NA_real_),
    
    layer1.2 = paste0(layer1, ">", layer2)
  )
