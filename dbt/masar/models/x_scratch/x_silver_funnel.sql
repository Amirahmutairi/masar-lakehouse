with trips as (

    select *
    from {{ ref('stg_trips') }}

)

select

    count(*) as total,

    sum(
        case
            when status != 'completed'
              or status is null
            then 1 else 0
        end
    ) as dropped_not_completed,

    sum(
        case
            when status = 'completed'
             and ended_at <= started_at
            then 1 else 0
        end
    ) as dropped_invalid_time,

    sum(
        case
            when status = 'completed'
             and ended_at > started_at
            then 1 else 0
        end
    ) as kept

from trips