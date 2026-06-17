{{ config(materialized='view') }}

WITH

rentals AS (
    SELECT *
    FROM {{ ref('drive_rentals_fct') }}
)

SELECT
    * EXCEPT(
        rental_cost_eur, paid_amount_eur, refunded_amount_eur, failed_amount_eur,
        wallet_paid_amount_eur, card_paid_amount_eur, discount_amount_eur,
        gross_revenue_eur, net_revenue_eur
    ),
    rental_cost_eur AS rental_cost,
    paid_amount_eur AS paid_amount,
    refunded_amount_eur AS refunded_amount,
    failed_amount_eur AS failed_amount,
    wallet_paid_amount_eur AS wallet_paid_amount,
    card_paid_amount_eur AS card_paid_amount,
    discount_amount_eur AS discount_amount,
    gross_revenue_eur AS gross_revenue,
    net_revenue_eur AS net_revenue
FROM rentals