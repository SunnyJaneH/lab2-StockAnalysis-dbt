{% snapshot snapshot_stock_data %}
{{
  config(
    target_schema='snapshot',
    unique_key="symbol || '_' || date || '_' || updated_at",
    strategy='timestamp',
    updated_at='updated_at',
    invalidate_hard_deletes=True
  )
}}
SELECT * FROM {{ ref('clean_stock_data') }}
{% endsnapshot %}