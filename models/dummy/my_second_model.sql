with source as (
    select * from {{ ref('my_first_model') }}
)

select *, my_favorite_number * 2 as my_favorite_number_doubled
from source