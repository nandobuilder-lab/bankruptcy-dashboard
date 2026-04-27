---
title: State Analysis
---

```sql states
select * from bankruptcy.states_latest
```

```sql states_sorted_yoy
select * from bankruptcy.states_latest order by yoy_pct desc
```

## Filings by State — Latest Annual

<BarChart
  data={states}
  x="state"
  y="total_all"
  swapXY=true
  title="Total Annual Filings by State"
  yFmt="num0"
/>

---

## Year-over-Year Growth by State

<BarChart
  data={states_sorted_yoy}
  x="state"
  y="yoy_pct"
  swapXY=true
  title="YoY Filing Growth (%)"
  yFmt="pct1"
  colorPalette={["#16a34a", "#dc2626"]}
/>

---

## Chapter Mix — Top 20 States

<BarChart
  data={states}
  x="state"
  y={["total_ch7", "total_ch13", "total_ch11"]}
  labels={["Ch.7", "Ch.13", "Ch.11"]}
  type="stacked"
  swapXY=true
  title="Chapter Mix by State"
  yFmt="num0"
/>

---

## Full State Table

<DataTable
  data={states}
  rows={55}
  columnLabels={{
    state: "State",
    total_all: "Total",
    total_ch7: "Ch.7",
    total_ch13: "Ch.13",
    total_ch11: "Ch.11",
    ch7_pct: "Ch.7 %",
    ch13_pct: "Ch.13 %",
    biz_ch11_pct: "Biz Ch.11 %",
    yoy_pct: "YoY %"
  }}
  fmt={{
    total_all: "num0",
    total_ch7: "num0",
    total_ch13: "num0",
    total_ch11: "num0",
    ch7_pct: "pct1",
    ch13_pct: "pct1",
    biz_ch11_pct: "pct1",
    yoy_pct: "pct1"
  }}
/>
