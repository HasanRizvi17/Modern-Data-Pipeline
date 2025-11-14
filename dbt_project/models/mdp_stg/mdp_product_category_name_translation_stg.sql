{{ 
    config(
        tags = ['lookup']
    ) 
}}

SELECT *
FROM {{ ref('mdp_product_category_name_translation_ext') }}
