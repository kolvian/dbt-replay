{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='order_id') }}

select order_id, ordered_at, updated_at, amount
from {{ ref('stg_orders') }}
{% if is_incremental() and not var('replay_fix', false) %}
where ordered_at > (select max(ordered_at) from {{ this }})
{% endif %}
