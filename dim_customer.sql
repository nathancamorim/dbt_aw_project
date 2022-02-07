with
    customerinfo as (
        select
            customer_sk
            , customerid	
            , personid
            , storeid
	

        from {{ ref('stg_customer') }}
    )

    , storeinfo as (
        select 
            storecustomerid
            , storename
        
        from {{ ref('stg_store') }}
    )

    , personinfo as (
        select
            businessentityid
            , fullname

        
        from {{ ref('stg_person') }}
    )

    , final as (
        select
            customerinfo.customer_sk
            , customerinfo.customerid
            , storeinfo.storecustomerid
            , storeinfo.storename
            , personinfo.businessentityid
            , personinfo.fullname
            , case when personinfo.fullname is not null
            then personinfo.fullname
            else storeinfo.storename
            end as customername
        
        from customerinfo
        left join storeinfo on customerinfo.storeid = storeinfo.storecustomerid
        left join personinfo on customerinfo.personid = personinfo.businessentityid
    )

select * from final
