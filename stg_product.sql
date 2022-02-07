with 
    source as (
        select
            productid
            , name as productname
            , rowguid
            , listprice  
            --, productnumber , makeflag , finishedgoodsflag , color
            --, safetystocklevel ,reorderpoint, standardcost , size , sizeunitmeasurecode
            --, weight, daystomanufacture, productline , class , style , productsubcategoryid, productmodelid , sellstartdate
            --, sellenddate, discontinueddate , modifieddate ,   
            --, _sdc_table_version
            --, _sdc_received_at	
            --, _sdc_sequence	
            --, _sdc_batched_at
 
        from {{ source('adventureworks_project', 'product') }}
    )

    , transformed_source as (
        select
            row_number() over (order by productid) as product_sk
            , *
        from source
    )

select * from transformed_source


