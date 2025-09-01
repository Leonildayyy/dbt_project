{% macro auditing(old_db, old_schema, old_identifier, new_model, pk) %}
  {% set old_relation = adapter.get_relation(
      database = old_db,
      schema = old_schema,
      identifier = old_identifier
  ) %}

  {% set dbt_relation = ref(new_model) %}

  {% if old_relation is none %}
    {{ log("❌ Old relation not found: " ~ old_schema ~ "." ~ old_identifier, info=True) }}
    select 'Missing table: ' || '{{ old_identifier }}' as error_message
  {% else %}
    {{ audit_helper.compare_relations(
        a_relation = old_relation,
        b_relation = dbt_relation,
        primary_key = pk
    ) }}
  {% endif %}
{% endmacro %}