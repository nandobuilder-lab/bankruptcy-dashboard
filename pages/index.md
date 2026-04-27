---
title: US Bankruptcy Filings Dashboard
---

```sql summary
select * from bankruptcy.summary
```

```sql national_annual
select * from bankruptcy.national_annual
```

```sql monthly
select * from bankruptcy.monthly order by period_end
```

<div class="mb-6">

US Courts quarterly bankruptcy filing statistics, combined with FRED macroeconomic data.
Data through **<Value data={summary} column="period_end" fmt="mmm yyyy"/>** &nbsp;·&nbsp;
Source: Administrative Office of the US Courts

</div>

## At a Glance

<BigValue
  data={summary}
  value="total_all"
  title="Annual Filings (12-month)"
  fmt="num0"
  comparison="yoy_pct"
  comparisonTitle="vs. prior year"
  comparisonFmt="pct1"
/>

<BigValue
  data={summary}
  value="total_ch11"
  title="Ch.11 Annual Filings"
  fmt="num0"
  comparison="ch11_yoy_pct"
  comparisonTitle="vs. prior year"
  comparisonFmt="pct1"
/>

<BigValue
  data={summary}
  value="total_ch7"
  title="Ch.7 (Liquidation)"
  fmt="num0"
/>

<BigValue
  data={summary}
  value="total_ch13"
  title="Ch.13 (Consumer Reorg.)"
  fmt="num0"
/>

---

## National Trend — 12-Month Rolling

<LineChart
  data={national_annual}
  x="period_end"
  y={["total_ch7", "total_ch13", "total_ch11"]}
  labels={["Ch.7 Liquidation", "Ch.13 Consumer Reorg.", "Ch.11 Business Reorg."]}
  title="Annual Filings by Chapter"
  yFmt="num0"
/>

---

## Monthly Filings (Last 36 Months)

<BarChart
  data={monthly}
  x="period_end"
  y="total_all"
  title="Monthly National Filings"
  yFmt="num0"
  colorPalette={["steelblue"]}
/>
