with
    source as (
        select
            addressid
            , stateprovinceid    	
            --, addressline1	
            --, addressline2	
            , city		
            --, postalcode	
            --, spatiallocation	
            --, rowguid	
            --, modifieddate
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            -- , _sdc_batched_at	

        from {{ source('adventureworks_project', 'address') }}
    )

select * from source
