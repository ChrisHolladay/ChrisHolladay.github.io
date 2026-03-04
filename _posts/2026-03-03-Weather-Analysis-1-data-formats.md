---
layout: post
title: "Scraping Weather Prediction Data - Part I"
description: "Walking through setting up the data lake table and scraping script for weather prediction data"
tags:
  - Blogging
  - SQL
  - Analysis
---

## Working with Weather Prediction Data

One of my current projects with my homelab, which you can read about in my last post if you're interested in the data collection/scraping process, is an analysis of the error convergence for our local weather forecasts. The quick and dirty is that there's a very convenient API from Open-Meteo that provides access to the weather forecasts on an hour-by-hour basis over up to the next seven days from the time of the query, so I've been pulling that data for some days now and this makes the seventh day since I put that into production on my homelab, so now I'm curious what it looks like.
That then begs the question: What _should_ your data look like for conducting this kind of analysis? There's a real dichotomy in how we format our data for analyses, and that's something I've run into a lot with folks in the work world. My education is in Statistics/Math and my work experience is mostly in private equity and healthcare (I know, everyone's two favorite industries), so that's meant that quite often I've had to be the person who translates needs/demands/wants between the data teams and the business teams (groups like FP&A, Accounting, Ops, etc.). Skip ahead if you've already heard this before, but business side folks almost always want their data in "wide" format, where your individual dimension-grain unit (or an observation unit, for the statisticians out there) has one row and all the the various time-point/fact-grain observations for that unit are in their own columns on that same row. On the other hand, data folks tend to prefer "long" formatted data, where there are many more rows, but each row is a single observation/time point. This conforms to the dimensional model we all learned early on in our data careers, and it also happens to be the necessary format for most statistical analysis packages that operate with each of these as a separate point.

But that descriptive primer doesn't really break through for most people; it all makes more sense if you see it, and I'll use some of my new weather data for an example.

### The "Wide" Format

Here's a little sample of the weather data from my logs, formatted wide:

+---------------------+------------+-------+-------+-------+-------+-------+-------+-------+
| prediction_time     | t1         | temp1 | temp2 | temp3 | temp4 | temp5 | temp6 | temp7 |
+---------------------+------------+-------+-------+-------+-------+-------+-------+-------+
| 2026-03-03 00:00:00 | 2026-02-25 | 66.02 | 48.38 |  NULL | 78.62 | 75.92 | 67.28 | 78.08 |
| 2026-03-03 01:00:00 | 2026-02-25 |  63.5 | 48.56 |  NULL | 76.46 | 74.84 | 64.22 | 75.38 |
| 2026-03-03 02:00:00 | 2026-02-25 | 60.26 | 48.92 |  NULL |  74.3 | 73.94 | 62.24 | 74.66 |
| 2026-03-03 03:00:00 | 2026-02-25 | 57.74 | 49.28 |  NULL | 73.94 | 72.86 | 61.16 | 72.68 |
| 2026-03-03 04:00:00 | 2026-02-25 | 56.12 | 49.64 |  NULL | 73.76 | 71.24 |  60.8 | 71.42 |
| 2026-03-03 05:00:00 | 2026-02-25 | 54.86 |    50 |  NULL |  72.5 | 70.88 | 62.24 | 70.34 |
| 2026-03-03 06:00:00 | 2026-02-25 | 53.96 | 50.18 |  NULL | 71.24 | 70.16 | 63.32 | 69.44 |
| 2026-03-03 07:00:00 | 2026-02-25 | 53.06 | 50.18 |  NULL | 69.26 | 68.72 | 62.96 | 68.36 |
| 2026-03-03 08:00:00 | 2026-02-25 | 52.52 |    50 |  NULL | 68.18 | 67.28 |  62.6 |  67.1 |
| 2026-03-03 09:00:00 | 2026-02-25 | 51.98 | 49.82 |  NULL |  67.1 | 66.02 | 62.06 | 65.66 |
| 2026-03-03 10:00:00 | 2026-02-25 | 51.62 | 49.64 |  NULL | 66.02 | 64.76 | 61.34 | 64.58 |
| 2026-03-03 11:00:00 | 2026-02-25 | 51.08 | 49.64 |  NULL |  65.3 | 63.68 | 61.16 |  65.3 |
| 2026-03-03 12:00:00 | 2026-02-25 | 51.26 | 49.82 |  NULL | 64.76 | 63.14 | 61.88 | 66.56 |
| 2026-03-03 13:00:00 | 2026-02-25 | 51.62 |    50 |  NULL | 64.04 | 63.14 | 62.06 |  67.1 |
| 2026-03-03 14:00:00 | 2026-02-25 | 52.34 | 50.18 |  NULL | 64.94 | 64.04 | 64.04 | 68.18 |
| 2026-03-03 15:00:00 | 2026-02-25 | 54.14 | 50.72 |  NULL | 67.28 | 65.12 | 66.02 | 69.44 |
| 2026-03-03 16:00:00 | 2026-02-25 | 57.92 |  51.8 |  NULL | 70.52 | 66.56 |  68.9 |  71.6 |
| 2026-03-03 17:00:00 | 2026-02-25 |  62.6 | 53.24 |  NULL | 74.12 | 68.54 | 70.52 | 72.68 |
| 2026-03-03 18:00:00 | 2026-02-25 | 66.56 | 54.32 |  NULL | 77.36 |  71.6 | 73.58 | 75.92 |
| 2026-03-03 19:00:00 | 2026-02-25 | 68.72 | 54.86 |  NULL | 79.88 | 78.08 | 75.74 | 78.26 |
| 2026-03-03 20:00:00 | 2026-02-25 | 70.16 |  55.4 |  NULL | 81.86 | 80.78 |  77.9 |  79.7 |
| 2026-03-03 21:00:00 | 2026-02-25 | 71.06 | 55.76 |  NULL | 83.48 |  82.4 | 78.44 | 80.42 |
| 2026-03-03 22:00:00 | 2026-02-25 |  71.6 |  56.3 |  NULL | 84.74 | 83.48 | 79.88 |  80.6 |
| 2026-03-03 23:00:00 | 2026-02-25 | 71.78 | 56.66 |  NULL | 83.66 | 82.22 | 80.78 | 80.06 |
+---------------------+------------+-------+-------+-------+-------+-------+-------+-------+

You can see that each row is a single time point, and all of the _forcecast_ time samples are within that same row. This makes it easier to observe changes for a single unit, and tangentially helps develop the intuition for within-unit variance (this variance distinction is a huge part of a multivariate ANOVA or a repeated measures ANOVA, but that's for another day). The biggest part of this is that it's appealing to the eye, and you can get more data on the screen of a regular PC monitor, which are generally much wider than they are tall.

### The "Tall" Format

This is how your local Data Engineer is accustomed to seeing data. Every row represents not a single forecast point, including all of the forecasts made over time about that point, but a single prediction made for a given forecast point; this means that for every prediction time point, there will be seven rows, because each day's hourly predictions appear in the seven preceding days' forecasts.
Here's a sample of that same data from above, formatted long (abbreviated because the 23 rows above a turn into 161 rows (23 * 7) in this format:

+----------+---------------------+------------+-------+
| Event_ID | Prediction_Time     | oDate      | Temp  |
+----------+---------------------+------------+-------+
|      150 | 2026-03-03 05:00:00 | 2026-02-25 | 54.86 |
|      294 | 2026-03-03 05:00:00 | 2026-02-26 |    50 |
|      414 | 2026-03-03 05:00:00 | 2026-02-28 |  72.5 |
|      558 | 2026-03-03 05:00:00 | 2026-03-01 | 70.88 |
|      702 | 2026-03-03 05:00:00 | 2026-03-02 | 62.24 |
|      846 | 2026-03-03 05:00:00 | 2026-03-03 | 70.34 |
|      151 | 2026-03-03 06:00:00 | 2026-02-25 | 53.96 |
|      295 | 2026-03-03 06:00:00 | 2026-02-26 | 50.18 |
|      415 | 2026-03-03 06:00:00 | 2026-02-28 | 71.24 |
|      559 | 2026-03-03 06:00:00 | 2026-03-01 | 70.16 |
|      703 | 2026-03-03 06:00:00 | 2026-03-02 | 63.32 |
|      847 | 2026-03-03 06:00:00 | 2026-03-03 | 69.44 |
|      152 | 2026-03-03 07:00:00 | 2026-02-25 | 53.06 |
|      296 | 2026-03-03 07:00:00 | 2026-02-26 | 50.18 |
|      416 | 2026-03-03 07:00:00 | 2026-02-28 | 69.26 |
|      560 | 2026-03-03 07:00:00 | 2026-03-01 | 68.72 |
|      704 | 2026-03-03 07:00:00 | 2026-03-02 | 62.96 |
|      848 | 2026-03-03 07:00:00 | 2026-03-03 | 68.36 |
+----------+---------------------+------------+-------+

You'll notice that in long-formatted data has individual event IDs, those are the table keys and each one identifies a single prediction. You don't generally use those in analysis, since they're just synthetic data used to uniquely identify a row in the database's memory store, but these IDs do get used in some industries for keying directly between fact tables (which you're not usually supposed to do in the classic dimensional model), but the pharma research world does things their own way.


### Querying For Each Format

Querying a fact table to get the long-formatted data is extremely easy, I got that data with this query:
```SQL
SELECT Event_ID
      , Prediction_Time
      , oDate
      , Temp
FROM coredb.WeatherPredictions
WHERE prediction_time IN ('2026-03-03 07:00:00', '2026-03-03 06:00:00','2026-03-03 05:00:00');
```

If you want to extract the wide-formatted data from a dimensionally-modeled database, you're going to have to do a bit more work. If you're using a database that supports the PIVOT command, that'll do the formatting (and aggregation, if you want to look at means, medians, etc. for a given cell) for you, although it bears mentioning that pivoting in SQL has a fairly steep learning curve. Most experienced devs have to tinker with it for an hour or two before getting it to work right.
If your database doesn't support PIVOT, then you're simply going to need to get comfortable copy-pasting a lot, because you're going to need to do a lot of inner joins, and the major issue here is that while PIVOT will create as many columns as there are values in your original field where you're pulling those values from, that kind of indefinite range definition isn't possible when you're the one definining all of the columns via inner joins. Here's the query I used to get that wide-formatted data I showed above:
```SQL
SELECT prediction_time, t1, temp1, temp2, temp3, temp4, temp5, temp6, temp7
FROM (
SELECT ROW_NUMBER() OVER (PARTITION BY wp1.prediction_time ORDER BY wp1.oDate ASC) AS rownum, 
wp1.prediction_time, 
wp1.oDate AS t1, wp1.temp as temp1, 
wp2.odate AS t2, wp2.temp as temp2,
wp3.odate AS t3, wp3.temp as temp3,
wp4.odate AS t4, wp4.temp as temp4,
wp5.odate AS t5, wp5.temp as temp5,
wp6.odate AS t6, wp6.temp as temp6,
wp7.odate AS t7, wp7.temp as temp7
FROM coredb.WeatherPredictions wp1 
LEFT JOIN coredb.WeatherPredictions wp2 ON SUBDATE(wp1.oDate,-1) = wp2.oDate AND wp1.prediction_time=wp2.prediction_time
LEFT JOIN coredb.WeatherPredictions wp3 ON SUBDATE(wp1.oDate,-2) = wp3.oDate AND wp1.prediction_time=wp3.prediction_time
LEFT JOIN coredb.WeatherPredictions wp4 ON SUBDATE(wp1.oDate,-3) = wp4.oDate AND wp1.prediction_time=wp4.prediction_time
LEFT JOIN coredb.WeatherPredictions wp5 ON SUBDATE(wp1.oDate,-4) = wp5.oDate AND wp1.prediction_time=wp5.prediction_time
LEFT JOIN coredb.WeatherPredictions wp6 ON SUBDATE(wp1.oDate,-5) = wp6.oDate AND wp1.prediction_time=wp6.prediction_time
LEFT JOIN coredb.WeatherPredictions wp7 ON SUBDATE(wp1.oDate,-6) = wp7.oDate AND wp1.prediction_time=wp7.prediction_time
) base WHERE rownum=1 AND prediction_time >= '2026-03-03';
```

In the wide case, you can see that I had to do a join for each of the seven columns for predictions on a given time point, each time incrementing the oDate. Inner joins are also an option here, but these will fully eliminate all rows where there's not a full suite of seven days for a given prediction time. If your fact table is quite large, you _really_ want to be able to join along indexed columns to speed this up, otherwise the time cost to run this can be quite substantial. 

That's it for the conversation about data formatting for analyses; next time I'll probably make a post about the actual analysis of error convergence. Maybe some pretty pictures, every statistician loves a nice plot!

P.S.

Taking a brief aside into the database optimization math side for a minute (skip this if it's not interesting to you!), self-joins like this are actually the nightmare situation from every CS student's Algorithms class: an exponentially-scaling complexity function. Assuming the join is only based on a single match condition, you need to do at most _n_x_n_ many comparisons in a single self-join for O(n^2) complexity, and then that's multiplied by _n_ again for each additional join to get O(n^(m-1)) for an analysis with _m_ many time points. The immense value of clustered analyses is that they dampen the complexity by a log factor (close to the natural log ln(), and while it's not exact, it's close enough for our purposes). The log function has a really special property with regards to exponential functions that makes it phenomenal for this case: ln(n^(m-1)) = (m-1)*ln(n). Then the _m-1_ drops off because it's a constant factor that we don't care about in complexity analysis, and we're left with almost the best-case scenario: O(ln(n)). This is one of the immense values of clustered indexes (is it indices?).

Another quick detour for the curious, "oDate" is database shorthand for "operational date", or the date that an actual event occurred/started. Another common term is "eDate", although this one's a bit more nebulous in application; I've seen it used for both "effective date", or the same use as oDate, and "end date", or the terminating date complementing a start date given by oDate. Both work.
