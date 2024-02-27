with source as (
    select * from {{ ref('my_first_model') }}
)

select
    *,
    my_favorite_number * 2 as my_favorite_number_doubled,
    my_favorite_number / 2 as my_favorite_number_halved
from source