{#- Generic tests (no package dependency, so `dbt build` needs no network). -#}

{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test accepted_range(model, column_name, min_value=none, max_value=none) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} is not null
  and (
    1 = 0
    {%- if min_value is not none %} or {{ column_name }} < {{ min_value }}{% endif %}
    {%- if max_value is not none %} or {{ column_name }} > {{ max_value }}{% endif %}
  )
{% endtest %}

{% test at_least_rows(model, min_rows=1) %}
select 1 as failure
from (select count(*) as n from {{ model }}) as c
where c.n < {{ min_rows }}
{% endtest %}
