with
    source as (
        select
            customerid
            , territoryid	
            , storeid	
            , personid
            , modifieddate	
            , rowguid			
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at	
            
        from {{ source('adventureworks_project', 'customer') }}
    )
       , transformed_source as (
        select
            row_number() over (order by customerid) as customer_sk
            , *
        from source
    )


select * from transformed_source
