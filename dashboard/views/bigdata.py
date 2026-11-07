"""
bigdata.py - Big Data Pipeline: dataset 5GB/10GB/20GB, Spark ETL, kualidade dadus, análize anomalia batch,
benchmark Pandas vs Spark, estatístika streaming
"""
import altair as alt
import pandas as pd
import psutil
import streamlit as st

import charts as C
import config
import data
import ui
from common import domain as D

SIZES = ["5GB", "10GB", "20GB"]

ui.page_header("Big Data Pipeline", "bigdata", "Batch layer: dataset CSV (data lake raw) → Spark ETL → Parquet curated + table MySQL. "
           "Speed layer: Bot → FastAPI → Kafka → Spark Streaming → MySQL.")

t1, t2, t3, t4, t5 = st.tabs([":material/inventory_2: Dataset", ":material/settings_suggest: Spark ETL & kualidade",
                              ":material/query_stats: Anomalia iha dadus batch",
                              ":material/compare_arrows: Pandas vs Spark", ":material/stream: Streaming"])

with t1:
    rows = []
    for lab in SIZES:
        m = data.manifest(lab)
        if m:
            rows.append({"Dataset": lab, "Size (GB)": m["bytes"] / 1024**3, "Records": m["rows"], "Meters": m["meters"],
                         "Part file": m["parts"], "% anomalia": 100 - m["anomaly_pct"].get("Normal", 100),
                         "Generation (s)": m["seconds"]})
    if rows:
        ds_tbl = pd.DataFrame(rows)
        st.dataframe(ds_tbl.style.format({"Records": "{:,}", "Meters": "{:,}", "% anomalia": "{:.2f}%",
                                          "Size (GB)": "{:.2f}", "Generation (s)": "{:.1f}"}),
                     width="stretch", hide_index=True)
        st.caption(f"Total {ds_tbl['Size (GB)'].sum():.1f} GB CSV · {ds_tbl.Records.sum():,} records · "
                   f"part file ~115 MB (hanesan block HDFS) · data lake iha `{config.DATA_DIR}`")
        big = next(lab for lab in reversed(SIZES) if data.manifest(lab))
        m = data.manifest(big)
        st.write("**Oinsá dadus jera (método aprezentasaun individuál / Exercicio 1):**")
        st.markdown("\n".join(f"- {s}" for s in m["seed_data"]) +
                    "\n- Meters aumenta to'o CSV to'o tamañu alvu; reading 1 kada oras ba tinan 2025 tomak (8,760 / meter)"
                    "\n- Problema kualidade hatama: 0.3 % missing, 0.2 % duplikadu, 0.1 % valór invalidu")
        st.write("**Anomalia ne'ebé jeradór hatama (ground truth):**")
        inj = pd.DataFrame([{"Tipu": D.ANOMALY_TET[k], "Oinsá": v,
                             f"% readings ({big})": m["anomaly_pct"].get(k, 0)} for k, v in m["injected_anomalies"].items()])
        st.dataframe(inj, width="stretch", hide_index=True)
    else:
        st.info("Seidauk iha dataset - hala'o data_generator.generate_dataset")

with t2:
    runs = data.q("SELECT id, job_name, engine, dataset, status, started_at, seconds, input_rows, output_rows, "
                  "input_bytes, throughput_rows_s, details, message FROM tbl_pipeline_runs ORDER BY id DESC")
    if runs.empty:
        st.info("Seidauk hala'o Spark ETL")
    else:
        st.dataframe(runs.drop(columns=["details"]).rename(columns={
            "job_name": "Job", "engine": "Engine", "dataset": "Dataset", "status": "Status", "started_at": "Start",
            "seconds": "Seconds", "input_rows": "Input records", "output_rows": "Output records", "input_bytes": "Bytes",
            "throughput_rows_s": "Records/s", "message": "Message"}), width="stretch", hide_index=True,
            column_config={"Records/s": st.column_config.NumberColumn(format="%.0f"),
                           "Seconds": st.column_config.NumberColumn(format="%.1f")})
        etl = runs[(runs.job_name == "spark_batch_etl") & (runs.status == "success")].drop_duplicates("dataset")
        if not etl.empty:
            names = sorted(etl.dataset.tolist(), key=lambda s: SIZES.index(s) if s in SIZES else 9)
            pick = st.selectbox("Haree etapa ETL", names, index=len(names) - 1)
            det = data.jloads(etl[etl.dataset == pick].iloc[0]["details"], {})
            tm = pd.DataFrame({"Etapa": list(det.get("timings_s", {})), "Segundu": list(det.get("timings_s", {}).values())})
            if not tm.empty:
                C.show(alt.Chart(tm).mark_bar(cornerRadiusEnd=4, color=C.color_of("Voltage Sag")).encode(
                    y=alt.Y("Etapa:N", sort=None, title=None), x=alt.X("Segundu:Q"),
                    tooltip=["Etapa", alt.Tooltip("Segundu:Q", format=".1f")]).properties(height=260,
                    title=f"Spark ETL {pick}: {det.get('cores')} core, Spark {det.get('spark_version')}"))
            st.caption(f"Train sample {det.get('train_sample_rows', 0):,} linha (Normal undersampled) · test sample "
                       f"{det.get('test_sample_rows', 0):,} linha (natural) · curated Parquet {det.get('curated_rows') or 0:,} linha")
        dq = data.q("SELECT dataset, check_name, column_name, n_rows, pct FROM tbl_data_quality WHERE n_rows > 0 "
                    "ORDER BY dataset, n_rows DESC")
        st.write("**Data Quality (Spark profiling, veracity)**")
        st.dataframe(dq.rename(columns={"dataset": "Dataset", "check_name": "Check", "column_name": "Column",
                                        "n_rows": "Records", "pct": "%"}), width="stretch", hide_index=True)

with t3:
    stats = data.q("SELECT dataset, dimension, dim_value, anomaly_type, n_readings FROM tbl_batch_anomaly_stats")
    if stats.empty:
        st.info("Seidauk iha análize batch")
    else:
        opts = sorted(stats.dataset.unique(), key=lambda s: SIZES.index(s) if s in SIZES else 9)
        ds = st.radio("Dataset", opts, index=len(opts) - 1, horizontal=True)
        s = stats[(stats.dataset == ds) & (stats.anomaly_type != D.NORMAL)].assign(anomaly_type=lambda d: C.tet(d.anomaly_type))
        a1, a2 = st.columns(2)
        # titles as text and one shared legend (municipality chart below): with a legend each, the
        # half-width charts squeeze the bars to a few pixels
        with a1:
            st.markdown("**Anomalia tuir oras** (sag barak iha kalan-tuku, over-voltage iha madrugada)")
            h = s[s.dimension == "hour"].assign(hour=lambda d: d.dim_value.astype(int))
            C.show(alt.Chart(h).mark_bar().encode(
                x=alt.X("hour:O", title="Oras", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("sum(n_readings):Q", title="Anomalous readings"),
                color=alt.Color("anomaly_type:N", scale=C.scale(), legend=None),
                tooltip=["hour", "anomaly_type", alt.Tooltip("n_readings:Q", format=",")]).properties(height=260))
        with a2:
            st.markdown("**Anomalia tuir fulan (2025)**")
            mth = s[s.dimension == "month"].assign(month=lambda d: d.dim_value.astype(int))
            C.show(alt.Chart(mth).mark_bar().encode(
                x=alt.X("month:O", title="Fulan", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("sum(n_readings):Q", title="Anomalous readings"),
                color=alt.Color("anomaly_type:N", scale=C.scale(), legend=None),
                tooltip=["month", "anomaly_type", alt.Tooltip("n_readings:Q", format=",")]).properties(height=260))
        st.markdown("**Anomalia tuir municipality** (kór tuir tipu anomalia - legend iha okos)")
        mu = s[s.dimension == "municipality"]
        C.show(C.stacked_hbar(mu.rename(columns={"dim_value": "Municipality"}), "Municipality", "n_readings", "anomaly_type",
                              title_x="Anomalous readings"))
        st.write("**Feature profile kada tipu anomalia (median, Spark percentile_approx)** - hatudu tanba sa feature sira fó sinál")
        fp = data.q("SELECT anomaly_type, feature, p50 FROM tbl_batch_feature_profile WHERE dataset = :d", d=ds)
        if not fp.empty:
            piv = fp.pivot(index="anomaly_type", columns="feature", values="p50").reindex(D.ANOMALY_TYPES)
            st.dataframe(piv.rename(index=D.ANOMALY_TET, columns=D.FEATURE_LABELS).style.format("{:.3f}"), width="stretch")
        st.write("**Meters ho oras anómala barak liu (suspeita Na'ok Eletrisidade / Kontador Aat / Karga Liu)**")
        top = data.q("SELECT meter_id, municipality, customer_type, anomaly_type, n_hours, first_ts, last_ts, lost_kwh_est "
                     "FROM tbl_batch_top_meters WHERE dataset = :d ORDER BY n_hours DESC LIMIT 25", d=ds)
        top["anomaly_type"] = C.tet(top["anomaly_type"])
        st.dataframe(top.rename(columns={"meter_id": "Meter", "municipality": "Municipality", "customer_type": "Tipu",
                                         "anomaly_type": "Anomalia", "n_hours": "Oras", "first_ts": "Hahú",
                                         "last_ts": "Remata", "lost_kwh_est": "kWh lakon (estimativa)"}),
                     width="stretch", hide_index=True)

with t4:
    b = data.q("SELECT engine, dataset_label, n_rows, size_mb, seconds, peak_memory_mb, run_no, result_check "
               "FROM tbl_benchmarks")
    if b.empty:
        st.info("Seidauk hala'o benchmark - `python -m pipeline.benchmark`")
    else:
        order = ["10k", "50k", "100k", "500k", "1M"] + SIZES
        b["failed"] = b.result_check.fillna("").str.startswith("FAILED")
        ok, fail = b[~b.failed], b[b.failed].drop_duplicates(["engine", "dataset_label"])
        avg = ok.groupby(["engine", "dataset_label"]).agg(seconds=("seconds", "mean"), memory=("peak_memory_mb", "mean"),
                                                          size_mb=("size_mb", "mean"), rows=("n_rows", "max"),
                                                          runs=("run_no", "count")).reset_index()
        ram_gb = psutil.virtual_memory().total / 1024**3
        eng = alt.Scale(domain=["pandas", "spark"], range=[C.color_of("Voltage Sag"), C.color_of("Over-Voltage")])
        eng_legend = alt.Legend(orient="bottom", title=None)
        x_vol = alt.X("dataset_label:N", sort=order, title="Volume", axis=alt.Axis(labelAngle=0))
        c1, c2 = st.columns(2)
        with c1:
            # titles as text: a Vega title inside a hidden tab gets clipped when the tab opens
            st.markdown("**Dataset size vs execution time (média 3 run)**")
            C.show(alt.Chart(avg).mark_line(point=alt.OverlayMarkDef(size=70), strokeWidth=2).encode(
                x=x_vol, y=alt.Y("seconds:Q", title="Execution Time (s)"),
                color=alt.Color("engine:N", scale=eng, legend=eng_legend),
                tooltip=["engine", "dataset_label", alt.Tooltip("seconds:Q", format=".2f"), "runs"]).properties(
                height=280))
        with c2:
            st.markdown("**Peak Memory (✕ = la konsege, memória la to'o)**")
            mem = avg.assign(gb=avg.memory / 1024)
            lines = alt.Chart(mem).mark_line(point=alt.OverlayMarkDef(size=70), strokeWidth=2).encode(
                x=x_vol, y=alt.Y("gb:Q", title="Peak Memory (GB)"),
                color=alt.Color("engine:N", scale=eng, legend=eng_legend),
                tooltip=["engine", "dataset_label", alt.Tooltip("gb:Q", format=".1f", title="GB")])
            ram = pd.DataFrame({"gb": [ram_gb], "t": [f"RAM komputador {ram_gb:.0f} GB"]})
            layers = [lines, alt.Chart(ram).mark_rule(strokeDash=[5, 4], color="#8a8a85").encode(y="gb:Q"),
                      alt.Chart(ram).mark_text(align="left", dx=4, dy=-7, color="#8a8a85", fontSize=11).encode(
                          y="gb:Q", x=alt.value(0), text="t:N")]
            if not fail.empty:
                fx = fail.assign(gb=fail.peak_memory_mb / 1024, status="out of memory")
                layers.append(alt.Chart(fx).mark_point(shape="cross", size=160, filled=True).encode(
                    x=x_vol, y="gb:Q", color=alt.Color("engine:N", scale=eng, legend=None),
                    tooltip=["engine", "dataset_label", "status", alt.Tooltip("gb:Q", format=".1f", title="GB")]))
            C.show(alt.layer(*layers).properties(height=280))

        vols = [o for o in order if o in set(b.dataset_label)]

        def cell(e, v, col):
            a = avg[(avg.engine == e) & (avg.dataset_label == v)]
            if not a.empty:
                return float(a[col].iloc[0])
            return "fail" if not fail[(fail.engine == e) & (fail.dataset_label == v)].empty else None

        tbl = []
        for v in vols:
            pd_s, sp_s = cell("pandas", v, "seconds"), cell("spark", v, "seconds")
            pd_m, sp_m = cell("pandas", v, "memory"), cell("spark", v, "memory")
            fmt_s = lambda x: "✕ out of memory" if x == "fail" else ("-" if x is None else f"{x:.2f}")
            fmt_m = lambda x: "✕" if x == "fail" else ("-" if x is None else f"{x / 1024:.1f}")
            both = isinstance(pd_s, float) and isinstance(sp_s, float)
            tbl.append({"Volume": v, "Pandas (s)": fmt_s(pd_s), "Spark (s)": fmt_s(sp_s),
                        "Speedup (Pandas/Spark)": f"{pd_s / sp_s:.2f}" if both else
                        ("Spark de'it mak konsege" if pd_s == "fail" and isinstance(sp_s, float) else "-"),
                        "Pandas peak (GB)": fmt_m(pd_m), "Spark peak (GB)": fmt_m(sp_m)})
        st.dataframe(pd.DataFrame(tbl), width="stretch", hide_index=True)
        st.caption("Speedup > 1 = Spark lais liu. Spark iha kustu hahú JVM (~10 s), entaun Pandas manán iha dadus ki'ik "
                   "(10k-1M). Pandas tenke karrega dataset tomak iha RAM (~4x tamañu CSV): iha komputador RAM "
                   f"{ram_gb:.0f} GB, Pandas la konsege prosesa 5 GB, 10 GB no 20 GB (benchmark para nia antes Windows "
                   "hahú swap). Spark prosesa partition-partition no spill ba disk (~13 GB RAM de'it).")
        st.success("Konkluzaun: dataset 5 GB, 10 GB no 20 GB → **Apache Spark de'it** (Pandas out of memory).", icon=":material/lightbulb:")

with t5:
    sb = data.q("SELECT started_at, n_messages, n_valid, n_invalid, n_anomalies, processing_ms, avg_latency_ms, "
                "throughput_mps FROM tbl_stream_batches ORDER BY id DESC LIMIT 600")
    if sb.empty:
        st.info("Seidauk iha batch streaming")
    else:
        sb["started_at"] = pd.to_datetime(sb["started_at"])
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Batch (ikus 600)", f"{len(sb):,}")
        k2.metric("Messages / batch", f"{sb.n_messages.mean():.0f}")
        k3.metric("Average processing", f"{sb.processing_ms.mean():.0f} ms")
        k4.metric("Average latency", f"{sb.avg_latency_ms.mean() / 1000:.2f} s")
        C.show(alt.Chart(sb).mark_line(strokeWidth=1.5, color=C.color_of("Voltage Sag")).encode(
            x=alt.X("started_at:T", title=None), y=alt.Y("avg_latency_ms:Q", title="Latency (ms)"),
            tooltip=[alt.Tooltip("started_at:T", format="%H:%M:%S"), alt.Tooltip("avg_latency_ms:Q", format=".0f")])
            .properties(height=200, title="Latency end-to-end (bot → MySQL) kada micro-batch"))
        st.caption(f"Invalid messages rejeita husi Spark validation: {int(sb.n_invalid.sum()):,} husi "
                   f"{int(sb.n_messages.sum()):,} ({sb.n_invalid.sum() / max(sb.n_messages.sum(), 1) * 100:.2f}%)")
