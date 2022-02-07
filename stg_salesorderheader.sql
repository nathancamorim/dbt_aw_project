with
    source as (
        select
            salesorderid
            , customerid	
            , salespersonid	
            , territoryid
            , billtoaddressid	
            , shiptoaddressid
            , shipmethodid	
            , currencyrateid 
            , creditcardid
	
            , orderdate	
            , duedate	
            , shipdate	
            , revisionnumber
            , status	
            , onlineorderflag	
            , purchaseordernumber	
            , accountnumber	
            , creditcardapprovalcode	
            , subtotal	
            , taxamt	
            , freight	
            , totaldue	
            , rowguid	
            , modifieddate

            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at	

        from {{ source('adventureworks_project', 'salesorderheader') }}
    )

    , transformed_source as (
        select
            row_number() over (order by salesorderid) as orderheader_sk
            , *
        from source
    )

select * from transformed_source
