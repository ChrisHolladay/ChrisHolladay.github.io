---
layout: post
title: "Scraping Weather Prediction Data - Part I"
description: "Walking through setting up the data lake table and scraping script for weather prediction data"
tags:
  - Blogging
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

This is how your local Data Engineer is accustomed to seeing data. Every row represents not a single prediction time point, but a combination of a prediction time point and the observation date that prediction was made; this means that for every prediction time point, there will be seven rows, because each day's hourly predictions appear in the seven preceding days' forecasts.
Here's a sample of that same data from above, formatted long (abbreviated because the 23 rows above a turn into 161 rows (23 * 7) in this format:

