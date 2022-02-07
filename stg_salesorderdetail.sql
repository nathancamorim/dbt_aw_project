with
    source as (
        select 
            salesorderdetailid	
            , salesorderid
            , productid
            , specialofferid
            , orderqty			
            , unitprice	
            , unitpricediscount	
            , carriertrackingnumber
            , rowguid	
            --, modifieddate
            --, carriertrackingnumber
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at	

        from {{ source('adventureworks_project', 'salesorderdetail') }}
    )

    , transformed_source as (
        select
            row_number() over (order by salesorderdetailid) as orderdetail_sk
            , *
        from source
    )

select * from transformed_source
