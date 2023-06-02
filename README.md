# dbt reference implementation for Github Archive

<img src="https://github.com/preset-io/dbt_github_archive_bigquery/assets/487433/91891250-821c-40b7-b9e7-8215382aeefe"/>

This is a quick and dirty reference implementation to make sense of the
GitHub public information made available by the
[GH Archive](https://www.gharchive.org/) through BigQuery public datasets.

That data is a bit rough in a bunch of yearly/monthly/daily archived tables
that are fairly large (TBs) and you probably want to bring only the orgs/repos
you care about in a single table, and hopefully do some decent incremental
loads to make this queryable.

This dbt project does all this:
- brings all the archived tables in one centralized table in your local BigQuery project
- partitions by day, does incremental loads
- allows you to select just the repos you need
- rebuils some state tables off of the events table
- parses out important information out of JSON blobs
- get rid of redundant or not-so-useful-for-analytics information
