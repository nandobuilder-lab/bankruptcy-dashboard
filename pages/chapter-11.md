---
title: Chapter 11 Deep Dive
---

```sql national_annual
select * from bankruptcy.national_annual
```

```sql national_quarterly
select * from bankruptcy.national_quarterly
```

```sql districts
select * from bankruptcy.ch11_districts
```

Chapter 11 reorganization is the most consequential filing type for credit and
portfolio risk professionals. Unlike Ch.7 (immediate liquidation) and Ch.13
(consumer repayment plan), Ch.11 involves complex multi-party restructurings
that affect lenders, bondholders, trade creditors, and equity holders.

---

## National Ch.11 Trend

<LineChart
  data={national_annual}
  x="period_end"
  y="total_ch11"
  title="Annual Ch.11 Filings (12-Month Rolling)"
  yFmt="num0"
  colorPalette={["#dc2626"]}
/>

<LineChart
  data={national_annual}
  x="period_end"
  y={["biz_ch11", "nonbiz_ch11"]}
  labels={["Business Ch.11", "Non-Business Ch.11"]}
  title="Business vs. Non-Business Ch.11"
  yFmt="num0"
/>

<LineChart
  data={national_annual}
  x="period_end"
  y="biz_ch11_pct"
  title="Business Ch.11 as % of All Business Filings"
  yFmt="pct1"
  colorPalette={["#f97316"]}
/>

---

## Quarterly Momentum — Ch.11

<DataTable
  data={national_quarterly}
  rows={15}
  columnLabels={{
    period_end: "Quarter",
    total_ch11: "Ch.11 Filings",
    ch11_qoq_pct: "QoQ",
    ch11_sqpy_pct: "SQPY"
  }}
  fmt={{
    total_ch11: "num0",
    ch11_qoq_pct: "pct1",
    ch11_sqpy_pct: "pct1"
  }}
  columns={["period_end", "total_ch11", "ch11_qoq_pct", "ch11_sqpy_pct"]}
/>

---

## Top Districts by Business Ch.11

Texas Southern and Delaware dominate corporate reorganizations due to judicial
expertise and debtor-friendly venue selection.

<BarChart
  data={districts}
  x="district"
  y="biz_ch11"
  swapXY=true
  title="Business Ch.11 Filings by District"
  yFmt="num0"
  colorPalette={["#dc2626"]}
/>

<BarChart
  data={districts}
  x="district"
  y="biz_ch11_pct"
  swapXY=true
  title="Business Ch.11 as % of All District Filings"
  yFmt="pct1"
  colorPalette={["#f97316"]}
/>

<DataTable data={districts} />
