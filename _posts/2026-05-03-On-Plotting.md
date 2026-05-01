
---
layout: post
title: "On Plotting - Moving Averages"
description: "A quick graphical examination of moving averages as smoothers"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## Background

I wrote a few days ago about plots and my affinity for moving averages as smoothers in a few cases, but I thought it might be worth writing a short post illustrating the differences in a plot due to moving averages. To that end, I want to look at four plots that will look awfully familiar if you skimmed my last post:

### No Averaging, Just Plotting Raw Data

<img width="1011" height="661" alt="image" src="https://github.com/user-attachments/assets/d123db94-cd80-4f53-a705-a84c9ea1aa32" />

This plot is very spikey and what we would call "jittery", in that it bounces around for a run even if all of the Y-values in that run are generally in the same small range (or, as an economist would say, the short-run mean is stable). The only plots most people have ever seen that will look like this are financial plots, probably market return plots, although hopefully the assets you're looking at don't have those wild spike (at least on the negative side).

### Two-Term Moving Average

<img width="1011" height="661" alt="image" src="https://github.com/user-attachments/assets/f4b8e4b8-f2be-43ef-86bf-5af8c301239e" />

It's a little bit smoother, but still pretty jittery. You'll notice that the spikes have been lowered slightly, this is because those brief spikes are n


### Four-Term Moving Average

<img width="1011" height="661" alt="Image" src="https://github.com/user-attachments/assets/16078dc6-c2ff-4880-9d4f-489409e7ff8f" />


### Eight-Term Moving Average

<img width="1011" height="661" alt="image" src="https://github.com/user-attachments/assets/5490b20d-0b3d-4f4a-9fa0-b60a970f8482" />

