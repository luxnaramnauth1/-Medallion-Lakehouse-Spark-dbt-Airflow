{# Expose the Delta tables written by the Spark Silver job to the Thrift server catalog. #}
{% macro register_silver_tables() %}
  {% if execute %}
    {% do run_query("create schema if not exists silver") %}
    {% for t in ['customers', 'products', 'orders', 'order_items'] %}
      {% do run_query("create table if not exists silver." ~ t ~ " using delta location '/opt/lakehouse/silver/" ~ t ~ "'") %}
    {% endfor %}
  {% endif %}
{% endmacro %}
