with
    source as (
        select
            businessentityid as storecustomerid
            --, salespersonid
            , name as storename
            --, modifieddate	
            --, rowguid		
            --, _sdc_table_version
            --, _sdc_received_at
            --, _sdc_sequence
            --, _sdc_batched_at
            
        from {{ source('adventureworks_project', 'store') }}
    )

select * from source
