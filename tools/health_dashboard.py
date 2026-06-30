"""
Lekwankwa Platform — Enterprise Health Monitor

Reads gs://lekwankwa-metadata/health/health_status.json (written hourly by
job-health-check) and renders a full operations dashboard covering all 33
deployed GCP components:

  16 Cloud Run Jobs   (10 scrapers + 5 metadata + 1 health)
  16 Cloud Schedulers (one per job, europe-west1)
   1 Cloud Function   (pit-disclosure-generator, GCS event trigger)

Run locally:
    streamlit run tools/health_dashboard.py

Deploy as always-on Cloud Run Service:
    gcloud run deploy lekwankwa-health \
        --source . \
        --region africa-south1 \
        --command streamlit \
        --args "run,tools/health_dashboard.py,--server.port=8080,--server.address=0.0.0.0"
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import streamlit as st
import pandas as pd

_META_BUCKET = "lekwankwa-metadata"

# ── Constants ─────────────────────────────────────────────────────────────

_STATUS_ICON = {
    "SUCCEEDED":   "🟢",
    "OK":          "🟢",
    "ACTIVE":      "🟢",
    "RUNNING":     "🔵",
    "FAILED":      "🔴",
    "MISSING":     "🔴",
    "NOT_DEPLOYED":"🔴",
    "API_ERROR":   "🔴",
    "ERROR":       "🔴",
    "DEGRADED":    "🟠",
    "STALE":       "🟡",
    "NEVER_RUN":   "⚪",
    "UNKNOWN":     "⚪",
    "CRITICAL":    "🔴",
    "HIGH":        "🟠",
}

_OVERALL_COLOR = {
    "OPERATIONAL": "#00c853",
    "DEGRADED":    "#ff6d00",
    "OUTAGE":      "#d50000",
}

_SCRAPER_LABELS = {
    "job-food-usa":    "Food Pricing — USA (BLS/USDA)",
    "job-wages-usa":   "Wages & Employment — USA (BLS/ALFRED)",
    "job-trade-usa":   "Trade Flows — USA (Census BOP)",
    "job-housing-usa": "Housing — USA (Census/BLS)",
    "job-imf":         "Global Macro — IMF WEO (QUAD_VINTAGE)",
    "job-eurostat":    "Eurostat — EU27 × 5 products (SDMX)",
    "job-ons":         "ONS — GBR × 5 products",
    "job-statcan":     "StatCan — CAN × 5 products",
    "job-abs":         "ABS — AUS × 5 products (SDMX)",
    "job-ssb":         "SSB — NOR × 5 products (PX-Web)",
}

_META_LABELS = {
    "job-quality-live":       "Quality Report — Live mode",
    "job-quality-archive":    "Quality Report — Archive mode",
    "job-coverage-manifest":  "Coverage Manifest",
    "job-release-calendar":   "Release Calendar",
    "job-pit-disclosure":     "PIT Disclosure",
}

_HEALTH_LABELS = {
    "job-health-check": "Platform Health Check",
}


# ── Data loading ───────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _load() -> dict:
    from google.cloud import storage
    blob = storage.Client().bucket(_META_BUCKET).blob("health/health_status.json")
    return json.loads(blob.download_as_text())


def _ts(iso: str | None, short: bool = False) -> str:
    if not iso:
        return "—"
    s = iso.replace("T", " ")[:19] + " UTC"
    if short:
        return iso.replace("T", " ")[:16] + " UTC"
    return s


def _age(iso: str | None) -> str:
    if not iso:
        return "—"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        diff = datetime.now(timezone.utc) - dt
        h = int(diff.total_seconds() // 3600)
        m = int((diff.total_seconds() % 3600) // 60)
        if h >= 24:
            return f"{h // 24}d {h % 24}h ago"
        if h >= 1:
            return f"{h}h {m}m ago"
        return f"{m}m ago"
    except Exception:
        return "?"


def _icon(status: str) -> str:
    return _STATUS_ICON.get(status, "⚪")


def _sparkline(recent: list[str]) -> str:
    """ASCII sparkline of last N run results."""
    if not recent:
        return "—"
    dots = []
    for s in reversed(recent):
        dots.append("●" if s == "SUCCEEDED" else "○")
    return " ".join(dots)


def _dur(seconds: int | None) -> str:
    if seconds is None:
        return "—"
    if seconds < 60:
        return f"{seconds}s"
    return f"{seconds // 60}m {seconds % 60}s"


# ── Page config ────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Lekwankwa Health Monitor",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Custom CSS for enterprise look
st.markdown("""
<style>
    .status-banner {
        padding: 16px 24px;
        border-radius: 8px;
        font-size: 22px;
        font-weight: 700;
        letter-spacing: 0.05em;
        text-align: center;
        margin-bottom: 16px;
    }
    .metric-card {
        background: #1e1e2e;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 12px 16px;
        margin-bottom: 8px;
    }
    .alert-critical {
        background: #3b0a0a;
        border-left: 4px solid #d50000;
        padding: 12px 16px;
        border-radius: 4px;
        margin-bottom: 8px;
    }
    .alert-high {
        background: #2d1a00;
        border-left: 4px solid #ff6d00;
        padding: 12px 16px;
        border-radius: 4px;
        margin-bottom: 8px;
    }
    .sla-number {
        font-size: 36px;
        font-weight: 800;
    }
    div[data-testid="stDataFrame"] { font-size: 13px; }
    .stTabs [data-baseweb="tab"] { font-size: 14px; font-weight: 600; }
</style>
""", unsafe_allow_html=True)


# ── Load data ──────────────────────────────────────────────────────────────

def main() -> None:
    # ── Header ─────────────────────────────────────────────────────────────
    col_title, col_refresh = st.columns([8, 1])
    col_title.markdown("## 📊 Lekwankwa Platform — Health Monitor")

    if col_refresh.button("↺ Refresh", use_container_width=True):
        _load.clear()
        st.rerun()

    try:
        data = _load()
    except Exception as exc:
        st.error(f"Cannot load health snapshot: {exc}")
        st.info("Run `job-health-check` to generate the first snapshot, or check GCS credentials.")
        st.stop()

    generated_at = data.get("generated_at", "")
    counts       = data.get("component_counts", {})
    sla          = data.get("sla", {})
    jobs         = data.get("cloud_run_jobs",     [])
    scheds       = data.get("schedulers",          [])
    funcs        = data.get("cloud_functions",     [])
    vault        = data.get("vault_freshness",     [])
    meta         = data.get("metadata_freshness",  [])
    alerts       = data.get("quality_alerts",      [])

    overall  = sla.get("overall", "UNKNOWN")
    color    = _OVERALL_COLOR.get(overall, "#888")

    # ── Platform status banner ─────────────────────────────────────────────
    banner_text = {
        "OPERATIONAL": "✅  ALL SYSTEMS OPERATIONAL",
        "DEGRADED":    "⚠️  PLATFORM DEGRADED",
        "OUTAGE":      "🚨  PARTIAL OUTAGE DETECTED",
    }.get(overall, "⚪  STATUS UNKNOWN")

    st.markdown(
        f'<div class="status-banner" style="background:{color}22;border:1px solid {color};">'
        f'{banner_text}'
        f'</div>',
        unsafe_allow_html=True,
    )

    st.caption(
        f"Snapshot: {_ts(generated_at)}  ({_age(generated_at)})  |  "
        f"Auto-refreshes every 5 min  |  "
        f"Total components: **{counts.get('total', '?')}** "
        f"({counts.get('cloud_run_jobs','?')} jobs + "
        f"{counts.get('schedulers','?')} schedulers + "
        f"{counts.get('cloud_functions','?')} function)"
    )

    # ── Active incidents banner ────────────────────────────────────────────
    crit = [a for a in alerts if a.get("severity") == "CRITICAL"]
    high = [a for a in alerts if a.get("severity") == "HIGH"]

    if crit or high:
        if crit:
            msg = " | ".join(
                f"{a['product']}/{a['country']} [{a['code']}]" for a in crit[:3]
            )
            st.error(f"🔴 **{len(crit)} CRITICAL alert(s) active** — {msg}")
        if high:
            msg = " | ".join(
                f"{a['product']}/{a['country']} [{a['code']}]" for a in high[:3]
            )
            st.warning(f"🟠 **{len(high)} HIGH alert(s) active** — {msg}")
    else:
        st.success("No active quality alerts.")

    st.divider()

    # ── SLA metrics row ────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)

    def _sla_metric(col, label, value, healthy, total):
        color_css = "#00c853" if value >= 90 else ("#ff6d00" if value >= 70 else "#d50000")
        col.markdown(
            f'<div style="text-align:center">'
            f'<div style="font-size:11px;color:#888;text-transform:uppercase;letter-spacing:.08em">{label}</div>'
            f'<div class="sla-number" style="color:{color_css}">{value:.0f}%</div>'
            f'<div style="font-size:11px;color:#aaa">{healthy}/{total} healthy</div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    _sla_metric(c1, "Job SLA",   sla.get("job_sla", 0),
                sla.get("healthy_jobs", 0), sla.get("total_jobs", 0))
    _sla_metric(c2, "Vault SLA", sla.get("vault_sla", 0),
                sla.get("fresh_vault", 0), sla.get("total_vault", 0))
    _sla_metric(c3, "Metadata",  sla.get("meta_sla", 0),
                sla.get("fresh_meta", 0), sla.get("total_meta", 0))

    n_failed = sum(1 for j in jobs   if j.get("status") == "FAILED")
    n_stale  = sum(1 for s in scheds if s.get("stale"))
    fn_ok    = all(f.get("status") == "OK" for f in funcs)

    c4.metric("Failed Jobs",    n_failed, delta=None,
              delta_color="inverse" if n_failed else "normal")
    c5.metric("Cloud Function", "OK" if fn_ok else "ISSUE",
              delta=None)

    st.divider()

    # ── Main tabs ──────────────────────────────────────────────────────────
    tab_jobs, tab_sched, tab_fn, tab_vault, tab_meta, tab_alerts = st.tabs([
        f"🖥️ Jobs ({len(jobs)})",
        f"⏱️ Schedulers ({len(scheds)})",
        f"⚡ Functions ({len(funcs)})",
        f"🗄️ Vault Freshness ({len(vault)})",
        f"📁 Metadata ({len(meta)})",
        f"⚠️ Alerts ({len(alerts)})",
    ])

    # ── TAB 1: Cloud Run Jobs ──────────────────────────────────────────────
    with tab_jobs:
        st.markdown("##### 33-Component Inventory — Cloud Run Jobs (16)")

        sub_scraper, sub_meta, sub_health = st.tabs([
            f"Scrapers ({len(_SCRAPER_LABELS)})",
            f"Metadata Tools ({len(_META_LABELS)})",
            f"Health (1)",
        ])

        def _render_jobs(tab, job_labels: dict):
            with tab:
                rows = []
                for j in jobs:
                    name = j.get("job", "")
                    if name not in job_labels:
                        continue
                    status    = j.get("status", "UNKNOWN")
                    recent    = j.get("recent_statuses", [])
                    rows.append({
                        "Status":      f"{_icon(status)} {status}",
                        "Job":         name,
                        "Description": job_labels.get(name, name),
                        "Last Run":    _ts(j.get("last_run"), short=True),
                        "Age":         _age(j.get("last_run")),
                        "Duration":    _dur(j.get("duration_s")),
                        "Success Rate": f"{j.get('success_rate_pct', '—')}%" if j.get('success_rate_pct') is not None else "—",
                        "Last 5 Runs": _sparkline(recent),
                    })
                    if status == "FAILED":
                        st.warning(
                            f"**{name}** last execution FAILED — "
                            f"`gcloud run jobs executions list --job={name} --region=africa-south1`"
                        )

                if rows:
                    st.dataframe(
                        pd.DataFrame(rows),
                        use_container_width=True,
                        hide_index=True,
                    )

        _render_jobs(sub_scraper, _SCRAPER_LABELS)
        _render_jobs(sub_meta,    _META_LABELS)
        _render_jobs(sub_health,  _HEALTH_LABELS)

    # ── TAB 2: Schedulers ─────────────────────────────────────────────────
    with tab_sched:
        st.markdown("##### Cloud Schedulers (16)  —  all in `europe-west1`")
        st.caption("Schedulers POST to Cloud Run Jobs API in africa-south1. "
                   "Stale = no attempt in >48h (except sched-imf: quarterly, threshold 95 days).")

        rows = []
        for s in scheds:
            stale  = s.get("stale", False)
            status = "STALE" if stale else "OK"
            age    = s.get("age_hours")
            rows.append({
                "Status":       f"{_icon(status)} {status}",
                "Scheduler":    s.get("scheduler", "?"),
                "GCP State":    s.get("state", "?"),
                "Last Attempt": _ts(s.get("last_attempt"), short=True),
                "Age":          f"{age:.0f}h ago" if age else "never",
                "Next Run":     _ts(s.get("next_run"), short=True),
                "Threshold":    f"{s.get('stale_threshold_hours', 48)}h",
            })
            if stale:
                st.warning(
                    f"**{s.get('scheduler')}** has not fired in "
                    f"{age:.0f}h (threshold: {s.get('stale_threshold_hours', 48)}h)"
                )

        if rows:
            st.dataframe(
                pd.DataFrame(rows),
                use_container_width=True,
                hide_index=True,
            )

    # ── TAB 3: Cloud Function ─────────────────────────────────────────────
    with tab_fn:
        st.markdown("##### Cloud Functions (1)")
        st.caption("Event-driven; fires on GCS object finalization in lekwankwa-vault. "
                   "Generates PIT disclosure docs on every new scraper upload.")

        for f in funcs:
            status = f.get("status", "UNKNOWN")
            c1, c2, c3, c4 = st.columns([1, 2, 2, 2])
            c1.write(f"{_icon(status)} **{status}**")
            c2.write(f"**{f.get('function')}**")
            c3.write(f"State: {f.get('state', '?')}")
            c4.write(f"Runtime: {f.get('runtime', '?')}")

            if status not in ("OK", "ACTIVE"):
                st.error(
                    f"**{f.get('function')}** is {status}. Re-deploy with: "
                    f"`bash deploy/04_cloud_run_jobs.sh` (Step 7)"
                )
            else:
                st.success(
                    f"{f.get('function')} is ACTIVE. "
                    f"Updated: {_ts(f.get('update_time'))}"
                )

    # ── TAB 4: Vault Freshness ────────────────────────────────────────────
    with tab_vault:
        st.markdown("##### Vault Data Freshness — `gs://lekwankwa-vault`")
        st.caption(
            "STALE = last vault write older than 2× the expected release lag for that product. "
            "Indicates the API source may be down or returning 0 rows."
        )

        if not vault:
            st.info("No vault data found — run a scraper job first.")
        else:
            stale_v = [v for v in vault if v.get("health") == "STALE"]
            ok_v    = [v for v in vault if v.get("health") == "OK"]

            if stale_v:
                st.error(f"{len(stale_v)} series stale — potential source API issue")

            col_a, col_b = st.columns(2)
            col_a.metric("Fresh",    len(ok_v),    f"{round(len(ok_v)/len(vault)*100)}%")
            col_b.metric("Stale",    len(stale_v), f"{round(len(stale_v)/len(vault)*100)}%")

            df = pd.DataFrame(vault)
            df.insert(0, "Status", df["health"].map(lambda h: f"{_icon(h)} {h}"))
            df["last_write"] = df["last_write"].str[:19].str.replace("T", " ")
            st.dataframe(
                df[["Status", "product", "country", "last_write",
                    "age_days", "stale_threshold_days"]].rename(columns={
                    "product":              "Product",
                    "country":              "Country",
                    "last_write":           "Last Write (UTC)",
                    "age_days":             "Age (days)",
                    "stale_threshold_days": "Stale at (days)",
                }),
                use_container_width=True,
                hide_index=True,
            )

    # ── TAB 5: Metadata Freshness ─────────────────────────────────────────
    with tab_meta:
        st.markdown("##### Metadata Folder Freshness — `gs://lekwankwa-metadata`")
        st.caption(
            "Each folder is updated by a dedicated Cloud Run Job on its schedule. "
            "STALE = last file updated more than 26h ago (health/ folder: 2h)."
        )

        for m in meta:
            h     = m.get("health", "UNKNOWN")
            age_h = m.get("age_hours")
            icon  = _icon(h)

            c1, c2, c3, c4, c5 = st.columns([1, 2, 3, 2, 2])
            c1.write(f"{icon} **{h}**")
            c2.write(f"**{m['label']}**")
            c3.code(m.get('folder', '?'))
            c4.write(_ts(m.get("last_update"), short=True))
            c5.write(f"{age_h:.1f}h ago" if age_h else "—")

    # ── TAB 6: Quality Alerts ─────────────────────────────────────────────
    with tab_alerts:
        st.markdown("##### Active Quality Alerts — from latest quality report")
        st.caption(
            "CRITICAL/HIGH findings from `job-quality-live` or `job-quality-archive`. "
            "The self-healing handler fires on these: Layer 1 retry → Layer 2 Claude diagnosis + email."
        )

        if not alerts:
            st.success("No CRITICAL or HIGH findings in the latest quality report.")
        else:
            for a in alerts:
                sev = a.get("severity", "?")
                div_class = "alert-critical" if sev == "CRITICAL" else "alert-high"
                st.markdown(
                    f'<div class="{div_class}">'
                    f'<strong>{_icon(sev)} {sev}</strong> &nbsp; '
                    f'{a.get("product","?")} / {a.get("country","?")} &nbsp; '
                    f'<code>[{a.get("code","?")}]</code><br>'
                    f'<span style="color:#ccc;font-size:13px">{a.get("message","")}</span><br>'
                    f'<span style="color:#666;font-size:11px">'
                    f'Report: {a.get("report","?")} &nbsp;|&nbsp; '
                    f'{_ts(a.get("report_ts"))}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    # ── Quick actions ──────────────────────────────────────────────────────
    with st.expander("⚙️  Quick Actions", expanded=False):
        st.markdown("**Trigger a job manually (run in Cloud Shell):**")
        job_choice = st.selectbox(
            "Select job",
            [j["job"] for j in jobs],
            label_visibility="collapsed",
        )
        st.code(
            f"gcloud run jobs execute {job_choice} "
            f"--region=africa-south1 "
            f"--project={data.get('project', 'fluted-alloy-498317-u0')}",
            language="bash",
        )

        st.markdown("**View job logs:**")
        st.code(
            f"gcloud run jobs executions list --job={job_choice} "
            f"--region=africa-south1 "
            f"--project={data.get('project', 'fluted-alloy-498317-u0')}",
            language="bash",
        )

        st.markdown("**Force health snapshot now:**")
        st.code(
            "gcloud run jobs execute job-health-check "
            "--region=africa-south1 "
            f"--project={data.get('project', 'fluted-alloy-498317-u0')}",
            language="bash",
        )

    # ── Footer ─────────────────────────────────────────────────────────────
    st.caption(
        "Lekwankwa Corporation  |  "
        "Health check: `job-health-check` (every hour)  |  "
        "Self-healing: Layer 1 Crawl4AI retry → Layer 2 Claude Sonnet 4.6 diagnosis  |  "
        "Vault: `gs://lekwankwa-vault`  |  Metadata: `gs://lekwankwa-metadata`"
    )


if __name__ == "__main__":
    main()
