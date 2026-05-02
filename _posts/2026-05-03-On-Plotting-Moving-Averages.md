---
layout: post
title: "On Plotting - Moving Averages"
description: "A quick graphical examination of moving averages as smoothers"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## On Plotting - Moving Averages

### Background

I wrote a few days ago about plots and my affinity for moving averages as smoothers in a few cases, but I thought it might be worth writing a short post illustrating the differences in a plot due to moving averages. To that end, I want to look at four plots that will look awfully familiar if you skimmed my last post:

### No Averaging, Just Plotting Raw Data

<img width="649" height="350" alt="image" src="https://github.com/user-attachments/assets/d123db94-cd80-4f53-a705-a84c9ea1aa32" />



This plot is very spikey and what we would call "jittery" or "noisy", in that it bounces around for a run even if all of the Y-values in that run are generally in the same small range[^1]. The only plots most people have ever seen that will look like this are financial plots, probably market return plots, although hopefully the assets you're looking at don't have those wild spike (at least on the negative side).

### Two-Term Moving Average

<img width="649" height="350" alt="image" src="https://github.com/user-attachments/assets/f4b8e4b8-f2be-43ef-86bf-5af8c301239e" />



It's a little bit smoother, but still pretty jittery. You'll notice that the spikes have mostly been attenuated; this is because those brief spikes that only last for one time point are now being averaged with the prior time point that is much closer to the long-run mean. If you believe that the data on a given plot is the result of a systematic generating process plus some "noise" or errors introduced by various outside sources like measurement errors or other latent variables, then what you're doing in this smoothing process is reducing the impact of noise by distributing it. If the observation at time *t* has a larger noise, then that noise will be mitigated when the points at *t* and *t-1* (which is hopefully less impacted by noise than point *t*, although that's not a guarantee if your noise is more due to latent variables than purely random error) are averaged together, but it will also bleed over when the points at *t* and *t+1* are averaged together. It's all about distributing the impact of the noise across multiple terms of the moving average.


### Four-Term Moving Average

<img width="649" height="350" alt="Image" src="https://github.com/user-attachments/assets/16078dc6-c2ff-4880-9d4f-489409e7ff8f" />



Now we can really see the smoothing taking effect, with four terms to distribute that noise. Some of the smaller spikes are mostly or totally removed, like the small one halfway through April 18th and the much larger spike at the top of April 25th. Anecdotally, April 25th was when our recent run of muggy and rainy weather hit, so that might account for the increased errors[^2] after that point.


### Eight-Term Moving Average

<img width="649" height="350" alt="image" src="https://github.com/user-attachments/assets/5490b20d-0b3d-4f4a-9fa0-b60a970f8482" />


Now this right here? This is a nice plot. Very smooth, easy to see trends over both the long run and individual short-run periods. In my experience, this type of plot is much more appealing to busy executives. One thing to be wary about is using too large of a moving average; most statisticians would be wary of an eight-term moving average because it will probably distribute the noise *too* well, but it works out here because our time interval is hourly, so we're only ever averaging together eight hours of prediction errors, and ambient outdoor temperatures don't generally change that much over the course of just a day.[^3]


[^1] Or, as an economist would say, the short-run mean is stable.
[^2] Greater errors cause variance to increase, and most people would just talk about this in terms of the variance rather than the specific error trends.
[^3] Except when there are major weather phenomena, I suppose, and it's probably worth noting that we had tornadoes here in north Texas this week. Whoops. Might be better to use a four- or five-term average, then.
