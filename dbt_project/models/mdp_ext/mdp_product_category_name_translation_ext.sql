{{ 
    config(
        tags = ['lookup']
    ) 
}}

SELECT *
FROM {{ source('mdp_raw', 'mdp_product_category_name_translation_raw') }}
