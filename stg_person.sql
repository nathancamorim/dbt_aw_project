with
    source as (
        select
            businessentityid
            , CONCAT(firstname, ' ', lastname) as fullname
            --, title
            , firstname
            --, middlename
            , lastname
            --, suffix
            --, persontype
            --, namestyle
            --, emailpromotion
            --, modifieddate
            --, _sdc_sequence
            --, _sdc_batched_at
            
        from {{ source('adventureworks_project', 'person') }}
    )

select * from source
