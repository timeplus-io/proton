# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair==5.5.0",
#     "marimo",
#     "polars[pyarrow]==1.26.0",
#     "sqlalchemy==2.0.40",
#     "sqlglot==26.12.1",
#     "timeplus-connect==0.8.16",
# ]
# ///

import marimo

__generated_with = "0.12.5"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(sqlalchemy):
    engine = sqlalchemy.create_engine("timeplus://default:@localhost:8123")
    return (engine,)


@app.cell(hide_code=True)
def _(engine, mo):
    _df = mo.sql(
        f"""
        CREATE EXTERNAL STREAM IF NOT EXISTS github_events(actor string,
                  created_at string,
                  id string,
                  payload string,
                  repo string,
                  type string
                 )
        SETTINGS type='kafka',
                 brokers='kafka.demo.timeplus.com:9092',
                 topic='github_events',
                 security_protocol='SASL_SSL',
                 username='demo',
                 password='demo123',
                 skip_ssl_cert_check=true,
                 data_format='JSONEachRow',
                 one_message_per_row=true
        """,
        output=False,
        engine=engine
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        f"""
        # Live GitHub Events
        👋 This is a live notebook, built with [Timeplus](https://github.com/timeplus-io/proton) and [marimo](https://marimo.io), showing streaming data from GitHub via a public facing Kafka broker from kafka.demo.timeplus.com:9092

        Simply run the following commands:
        ```bash
        curl https://astral.sh/uv/install.sh | sh
        curl https://install.timeplus.com/oss | sh
        ./proton server&
        uvx marimo run --sandbox github.py
        ```
        """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    range=mo.ui.slider(start=1, stop=10, step=1,label="View data for X minutes:", show_value=True)
    return (range,)


@app.cell
def _(mo):
    cntRefresh = mo.ui.refresh(options=["2s"],default_interval="2s")
    return (cntRefresh,)


@app.cell
def _(df_cnt, get_count, mo, set_count):
    _new_count=df_cnt["cnt"][0]
    _diff=_new_count-get_count()
    set_count(_new_count)
    mo.stat(_new_count,label="In the Kafka topic",caption=f"{_diff} events", direction="increase" if _diff >= 0 else "decrease")
    return


@app.cell
def _(df_last, mo):
    from datetime import datetime, timezone
    event_time=datetime.strptime(df_last["created_at"][0],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    current_time = datetime.now(timezone.utc)
    mo.stat(f"{datetime.strftime(event_time,"%H:%M:%S %m/%d")} (UTC)",label="Last Event",caption=f"{int((current_time - event_time).total_seconds()/60)} mins ago")
    return current_time, datetime, event_time, timezone


@app.cell
def _(mo):
    refresh = mo.ui.refresh(label="Refresh",options=["5s", "10s", "30s"])
    return (refresh,)


@app.cell
def _(chart_repos, chart_types, mo, range, refresh):
    mo.vstack([mo.hstack([range,refresh]),mo.hstack([chart_types,chart_repos],widths=[0,1])])
    return


@app.cell(hide_code=True)
def _(chart_types):
    _type=' '
    if chart_types.selections.get("select_point"):
        _array=chart_types.selections["select_point"].get("type",None)
        if _array:
            _type=f"WHERE type='{_array[0]}'"
    typeWhere=_type
    return (typeWhere,)


@app.cell
def _(mo):
    mo.md(r"""## Sample Events""")
    return


@app.cell
def _(engine, mo, refresh):
    df_last = mo.sql(
        f"""
        -- {refresh.value}
        SELECT * FROM github_events order by _tp_time desc limit 3 settings seek_to='-10s'
        """,
        engine=engine
    )
    return (df_last,)


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import sqlalchemy
    import altair as alt
    return alt, mo, sqlalchemy


@app.cell
def _(alt, df_type, mo):
    chart_types = mo.ui.altair_chart(
        alt.Chart(df_type, height=150, width=150)
        .mark_arc()
        .encode(theta="cnt", color="type"),
        legend_selection=False
    )
    return (chart_types,)


@app.cell
def _(alt, df_hotrepo, mo):
    chart_repos = mo.ui.altair_chart(
        alt.Chart(df_hotrepo, height=200)
        .mark_bar()
        .encode(x='cnt',
                y=alt.Y('repo',sort=alt.EncodingSortField(field='cnt',order='descending')),)
    )
    return (chart_repos,)


@app.cell
def _(engine, mo, range, refresh, typeWhere):
    df_hotrepo = mo.sql(
        f"""
        -- {refresh.value}
        with cte as(SELECT top_k(repo,10,true) as a FROM github_events {typeWhere} limit 1 SETTINGS seek_to='-{range.value}m')
        select a.1 as repo,a.2 as cnt from cte array join a
        """,
        engine=engine
    )
    return (df_hotrepo,)


@app.cell(hide_code=True)
def _(engine, mo, range, refresh):
    df_type = mo.sql(
        f"""
        -- {refresh.value}
        with cte as(SELECT top_k(type,10,true) as a FROM github_events limit 1 SETTINGS seek_to='-{range.value}m')
        select a.1 as type,a.2 as cnt from cte array join a
        """,
        engine=engine
    )
    return (df_type,)


@app.cell
def _(cntRefresh):
    cntRefresh.style({"display": None})
    return


@app.cell(hide_code=True)
def cell_cnt(cntRefresh, engine, mo):
    df_cnt = mo.sql(
        f"""
        -- {cntRefresh.value}
        SELECT count() as cnt FROM github_events
        """,
        engine=engine
    )
    return (df_cnt,)


@app.cell
def _(mo):
    get_count, set_count = mo.state(0)
    return get_count, set_count


if __name__ == "__main__":
    app.run()
