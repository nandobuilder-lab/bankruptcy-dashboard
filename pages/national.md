---
title: National Trends
---

```sql national_annual
select * from bankruptcy.national_annual
```

```sql national_quarterly
select * from bankruptcy.national_quarterly
```

```sql monthly
select * from bankruptcy.monthly order by period_end
```

## Annual Filings — All Chapters

<LineChart
  data={national_annual}
  x="period_end"
  y="total_all"
  title="Total Annual Filings (12-Month Rolling)"
  yFmt="num0"
/>

<LineChart
  data={national_annual}
  x="period_end"
  y={["total_ch7", "total_ch13"]}
  labels={["Ch.7", "Ch.13"]}
  title="Ch.7 vs Ch.13"
  yFmt="num0"
/>

<LineChart
  data={national_annual}
  x="period_end"
  y="total_ch11"
  title="Ch.11 Annual Filings"
  yFmt="num0"
  colorPalette={["#dc2626"]}
/>

---

## Quarterly Momentum

Each quarter compared to the prior quarter (QoQ) and to the same quarter one year ago (SQPY).
Q4 is structurally the weakest quarter; negative QoQ in Q4 does not indicate a trend reversal.

<DataTable
  data={national_quarterly}
  rows={15}
  fmt={{
    total_all: "num0",
    qoq_pct: "pct1",
    sqpy_pct: "pct1",
    total_ch11: "num0",
    ch11_qoq_pct: "pct1",
    ch11_sqpy_pct: "pct1"
  }}
  columnLabels={{
    period_end: "Quarter",
    total_all: "3-Month Filings",
    qoq_pct: "QoQ",
    sqpy_pct: "SQPY",
    total_ch11: "Ch.11",
    ch11_qoq_pct: "Ch.11 QoQ",
    ch11_sqpy_pct: "Ch.11 SQPY"
  }}
/>

---

## Monthly Cadence

<BarChart
  data={monthly}
  x="period_end"
  y={["total_ch7", "total_ch13", "total_ch11"]}
  labels={["Ch.7", "Ch.13", "Ch.11"]}
  title="Monthly Filings by Chapter (Last 36 Months)"
  type="stacked"
  yFmt="num0"
/>
