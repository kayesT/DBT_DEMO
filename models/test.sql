select *
FROM {{ source('demo', 'bike') }}

LIMIT 10