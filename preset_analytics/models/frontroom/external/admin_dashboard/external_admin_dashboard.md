{% docs admin_dashboard %}
# Admin Dashboard
The set of models in the `admin_dashboard` sub-dir are used to report the usage of individual teams via Preset Manager's admin page.

## Template Dashboard
The [linked dashboard](https://2cad1810.us1a.app.preset.io/superset/dashboard/preset-metadata/?native_filters_key=o70f771nBw0xc0OGiukznUXX-hXsyLeLGeMfSqENjOh9cc6BHeSdlGXtlxfoRjdf) is the template for
which the Team Usage Admin Dashboard is based on.

### Tables used in template
*Users*
- `core`.`egaf_active_user`
- `core`.`manager_user`
- `core`.`egaf_growth_accounting`

*Dashboards*
- `core`.`egaf_active_entity`
- `core`.`superset_event_log`
- `core`.`superset_dashboard`

*Charts*
- `core`.`superset_chart`
- `core`.`egaf_active_entity`

{% enddocs %}
