# ProectNeo2
https://disk.yandex.ru/d/FMNnA2auAXJF_w
# 2.1 Дубликаты:
WITH duplicates AS (
  SELECT 
      client_rk,
      effective_from_date
  FROM dm.client
  GROUP BY client_rk, effective_from_date
  HAVING COUNT(*) > 1)

DELETE FROM dm.client
WHERE (client_rk, effective_from_date) IN (
    SELECT client_rk, effective_from_date
    FROM duplicates
)

select * from dm.client

# 2.2 
CREATE OR REPLACE PROCEDURE update_loan_holiday_info() AS $$
BEGIN
    DELETE FROM dm.loan_holiday_info;
    INSERT INTO dm.loan_holiday_info (
        deal_rk, effective_from_date, effective_to_date, agreement_rk,account_rk, client_rk,
        department_rk, product_rk, product_name, deal_type_cd, deal_start_date,
        deal_name, deal_number, deal_sum, loan_holiday_type_cd,
        loan_holiday_start_date, loan_holiday_finish_date,
        loan_holiday_fact_finish_date, loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    )
    SELECT 
        d.deal_rk, lh.effective_from_date, lh.effective_to_date, d.agreement_rk,d.account_rk ,d.client_rk,
        d.department_rk, d.product_rk, p.product_name, d.deal_type_cd, d.deal_start_date,
        d.deal_name, d.deal_num AS deal_number, d.deal_sum, lh.loan_holiday_type_cd,
        lh.loan_holiday_start_date, lh.loan_holiday_finish_date,
        lh.loan_holiday_fact_finish_date, lh.loan_holiday_finish_flg,
        lh.loan_holiday_last_possible_date
    FROM 
        rd.deal_info d
    LEFT JOIN 
        rd.loan_holiday lh ON d.deal_rk = lh.deal_rk AND d.effective_from_date = lh.effective_from_date
    LEFT JOIN 
        rd.product p ON p.product_rk = d.product_rk AND p.effective_from_date = d.effective_from_date;
END;
$$ LANGUAGE plpgsql;


# 2.3
WITH account_balance_corrected AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        ab1.account_in_sum,
        ab1.account_out_sum,
        COALESCE(ab2.account_out_sum, 0) AS previous_day_account_out_sum,
        CASE 
            WHEN ab1.account_in_sum <> COALESCE(ab2.account_out_sum, 0) THEN COALESCE(ab2.account_out_sum, 0)
            ELSE ab1.account_in_sum
        END AS corrected_account_in_sum
    FROM rd.account_balance ab1
    LEFT JOIN rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                                      AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
)
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    corrected_account_in_sum
FROM account_balance_corrected;
--
WITH account_balance_corrected AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        ab1.account_in_sum,
        ab1.account_out_sum,
        COALESCE(ab2.account_in_sum, 0) AS next_day_account_in_sum,
        CASE 
            WHEN ab1.account_out_sum <> COALESCE(ab2.account_in_sum, 0) THEN COALESCE(ab2.account_in_sum, 0)
            ELSE ab1.account_out_sum
        END AS corrected_account_out_sum
    FROM rd.account_balance ab1
    LEFT JOIN rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                                      AND ab1.effective_date + INTERVAL '1 day' = ab2.effective_date
)
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    corrected_account_out_sum
FROM account_balance_corrected;



--
UPDATE rd.account_balance ab
SET account_in_sum = abc.corrected_account_in_sum
FROM (
    WITH account_balance_corrected AS (
    SELECT 
        ab1.account_rk,
        ab1.effective_date,
        ab1.account_in_sum,
        ab1.account_out_sum,
        COALESCE(ab2.account_out_sum, 0) AS previous_day_account_out_sum,
        CASE 
            WHEN ab1.account_in_sum <> COALESCE(ab2.account_out_sum, 0) THEN COALESCE(ab2.account_out_sum, 0)
            ELSE ab1.account_in_sum
        END AS corrected_account_in_sum
    FROM rd.account_balance ab1
    LEFT JOIN rd.account_balance ab2 ON ab1.account_rk = ab2.account_rk 
                                      AND ab1.effective_date = ab2.effective_date + INTERVAL '1 day'
)
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    corrected_account_in_sum
FROM account_balance_corrected
) AS abc
WHERE ab.account_rk = abc.account_rk
  AND ab.effective_date = abc.effective_date;

--
CREATE OR REPLACE PROCEDURE reload_account_balance_turnover ()
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM dm.account_balance_turnover;
    INSERT INTO dm.account_balance_turnover (account_rk, currency_name, department_rk, effective_date, account_in_sum, account_out_sum)
    SELECT 
        a.account_rk,
        COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
        a.department_rk,
        ab.effective_date,
        ab.account_in_sum,
        ab.account_out_sum
    FROM rd.account a
    LEFT JOIN rd.account_balance ab ON a.account_rk = ab.account_rk
    LEFT JOIN dm.dict_currency dc ON a.currency_cd = dc.currency_cd;
END;
$$;
