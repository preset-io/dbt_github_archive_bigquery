# DBT Project for our BigQuery data warehouse

Find the [DBT-generated project documentation here](https://reimagined-happiness-d99dbd7a.pages.github.io/#!/overview)

DBT is scheduled to run daily in [Jenkins](https://jenkins.devops.preset.zone/me/my-views/view/all/job/preset-io/job/dataeng/)
(requires VPN access).

## Environments (targets)

There are 2 environments (set up as `target` in DBT, see in the
recommended `profiles.yml` below). `dev` (the default) and `prod`.
Jenkins runs against `master`, against the `prod` target. Locally you
should run default against `dev`. These 2 contexts target different
BigQuery projects: `prod` maps to `preset-cloud-dbt` and `dev` maps to
`preset-cloud-dev-dbt`

Knowing this, a simple `dbt run` will execute against `dev`. To execute
against `prod`, you should probably just merge to master and trigger on
Jenkins. Alternatively, you can `dbt run --target prod` if needed,
but that's not recommended or ideal.


## Getting set up locally

1. Install `dbt` and other deps `pip install -r requirements.txt`
1. Install pre-commit hooks `pre-commit install`
1. You'll need 2 files in your `~/.dbt/` folder:

`~/.dbt/profiles.yml`

```yaml
default:
  target: dev
  outputs:
    prod:
      type: bigquery
      method: service-account
      schema: core
      keyfile: /Users/max/.dbt/admin-dbt.json
      project: preset-cloud-dbt
      threads: 1
      timeout_seconds: 300
      location: us-west2
      priority: interactive
    dev:
      type: bigquery
      method: service-account
      schema: core
      keyfile: /Users/max/.dbt/dev-dbt.json
      project: preset-cloud-dev-dbt
      threads: 1
      timeout_seconds: 300
      location: us-west2
      priority: interactive
```


Find the file in [1password dev vault](https://my.1password.com/vaults/nonampeuhfe6nfha66o35vl5oa/allitems/pjax3q33y5by7jdhted4sbl63i),
the file under `preset-cloud-dev-dbt Admin service account` should be copied to this location `~/.dbt/dbt-dev-creds.json` and look something like this:

```json
{
      "type": "service_account",
      "project_id": "{{ REDACTED }}",
      "private_key_id": "{{ REDACTED }}",
      "private_key": "{{ REDACTED }}",
      "client_email": "{{ REDACTED }}",
      "client_id": "{{ REDACTED }}",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bigquery-production-data-viewe%40preset-cloud-analytics.iam.gserviceaccount.com"
}
```

If needed, the infra vault is where you'd find the connection for the `prod` target

1. That's all you should need, if you need to actually run in prod from

```bash
# install dbt deps  (dbt-utils)
dbt deps
#  Run dbt `dbt run` with 8 threads!
dbt run --threads 8
```

## Publish the docs
```bash
cd {REPO}/dbt/preset_analytics
dbt docs generate --threads 8
# delete the branch if it exists
git branch -D dbt-docs
# create from current location
git checkout -b dbt-docs
cp -r target/ ../../docs
# remove gitignore so that docs/ can be added / committed
rm ../../.gitignore
git add ../../docs/
git commit --no-verify -a -m "fresh docs"
git push origin dbt-docs -f
# wait a few minutes
```

## How we `dbt run` at Prest
We use incremental models that complicate how data is stored in our warehouse.
When an analyst, engineer, or automated script runs dbt, there are several global variable available for configuration.

Options are the following:
 - date_offset (default): generates a date range based on inc_dt_offset var
  ex: `dbt run --vars '{inc_dt_range_strategy: date_offset,  inc_dt_offset_days: 7}'`
 - range_select: user provided range of dates to replace
  ex: `dbt run --vars '{inc_dt_range_strategy: range_select, inc_dt_range_start_dt: DATE("2022-01-01"), inc_dt_range_end_dt: CURRENT_DATE() }'`
 - list_select: user provided list of dates to replace
  ex: `dbt run --vars '{inc_dt_range_strategy: list_select, inc_dt_list_select: [DATE("2022-01-01"), DATE("2022-01-02")]}'`
 - catchup: all dates greater than or equal to the max of the currently model
  ex: `dbt run --vars '{inc_dt_range_strategy: catchup}'`
