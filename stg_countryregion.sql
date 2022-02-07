with
    source as (
        select
            countryregioncode
            , name as countryname
            , modifieddate
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at

        from {{ source('adventureworks_project', 'countryregion') }}
    )

select * from source
