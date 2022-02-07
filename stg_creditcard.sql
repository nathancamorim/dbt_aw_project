with
    source as (
        select 
            creditcardid	
            , cardtype		
            --, expmonth	
            --, expyear	
            --, modifieddate
            , cardnumber	
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at

        from {{ source('adventureworks_project', 'creditcard') }}
    )

    , transformed_source as (
        select
            row_number() over (order by creditcardid) as creditcard_sk 
            , * 
        from source
    )

select * from transformed_source
