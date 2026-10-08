{#- Reusable generic data tests. -#}

{% test is_positive(model, column_name) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} <= 0
{% endtest %}


{% test value_between(model, column_name, min_value, max_value) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}


{#- Fails when fewer than `at_least` (0-1) of the rows have a non-null value. -#}
{% test not_null_proportion(model, column_name, at_least) %}
    select
        count({{ column_name }}) * 1.0 / nullif(count(*), 0) as non_null_share
    from {{ model }}
    having count({{ column_name }}) * 1.0 / nullif(count(*), 0) < {{ at_least }}
{% endtest %}


{#- Row-count reconciliation between two relations (optionally filtered). -#}
{% test equal_row_count(model, compare_model, compare_filter="1 = 1") %}
    with model_rows as (
        select count(*) as n from {{ model }}
    ),

    compare_rows as (
        select count(*) as n from {{ compare_model }} where {{ compare_filter }}
    )

    select
        model_rows.n as model_count,
        compare_rows.n as compare_count
    from model_rows
    cross join compare_rows
    where model_rows.n <> compare_rows.n
{% endtest %}


{#- Fails for every combination of `columns` that appears more than once. -#}
{% test unique_combination_of_columns(model, columns) %}
    select
        {{ columns | join(', ') }},
        count(*) as occurrences
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}
