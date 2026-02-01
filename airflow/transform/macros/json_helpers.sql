-- Macro to read JSON files from S3 using DuckDB's read_json function
-- Usage: {{ read_json_from_s3('s3://bucket/path/to/*.json') }}

{% macro read_json_from_s3(s3_path) %}
  SELECT 
    filename,
    json_array_length(json_extract_string(data, '$')) as record_count,
    data
  FROM read_json_auto('{{ s3_path }}', 
    format='array',
    ignore_errors=true
  ) AS t(data, filename)
{% endmacro %}

-- Macro to extract nested JSON fields
{% macro extract_json_field(json_col, path, field_type='VARCHAR') %}
  CAST(json_extract_string({{ json_col }}, '{{ path }}') AS {{ field_type }})
{% endmacro %}

-- Macro to parse match ID from filename
-- DuckDB doesn't have SPLIT_PART, so we use SUBSTR and STRPOS
{% macro extract_match_id_from_filename(filename) %}
  TRY_CAST(
    SUBSTR(
      {{ filename }},
      STRPOS({{ filename }}, '/') + 1,
      STRPOS(SUBSTR({{ filename }}, STRPOS({{ filename }}, '/') + 1), '_') - 1
    ) AS INTEGER
  )
{% endmacro %}
