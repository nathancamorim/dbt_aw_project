with
    staging_product as (
        select
            product_sk
            , productid	
            , productname	
	

        from {{ ref('stg_product') }}
    )

select * from staging_product
