---
layout: post
title: "On Plotting - The Magic of Moving Averages Pt. II "
description: "A quick graphical examination of discretizing and bucketing variance"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## On Moving Averages, Informed by Sub-Aggregated Variance Visualizations

### Background

A little while ago, I wrote a blog post about my affinity for moving average plots, and I used some data I've been collecting on weather predictions here in Dallas over the last few months. I've continuted collecting that data, and a little bit of a pattern has started to emerge where I think it's useful to add context using one of my other favorite plots for time series data: Sub-Aggregated Variance plots. I'm not sure if there's a better name for these plots, but that's what an old boss of mine in the finance world called them. I never encountered this type of plot in grad school, but if anyone knows what the "right" name for this type of plot is, I'd love to know.
Anyway, here's what the most recent iteration of that 8-term moving average error plot looks like:

<img width="500" height="362" alt="Image" src="https://github.com/user-attachments/assets/967660e7-b702-4907-924a-9caa81afca58" />

As a reminder, the darker lines are closer to the actual date of the prediction, so they'd be expected to generally have a lower variance than the lighter lines, which have a greater standoff from the actual observation. Each line is a moving average of eight time points, so the ERR2 line for 2026/05/14 11:00:00 is the average of the errors for the predictions made on 2026/05/09 (7-2 = 5 days prior to the observation date), for the eight hourly predictions points made for the hours from 4:00 through 11:00.

Now you can kind of see some variance patterns emerging based on where the spikes pop up, and it'll be fun in the future to pull history of humidity and precipitation and actual temperature to see if those impact predictive accuracy at various time horizons, but for now the interest is just in a basic graphical heuristic for assessing the variance of this predictor in practice. To that end, we can "bucket" the variance to each day, and plot that as a value for each day. You can also do a moving variance bucket where the value shown at time point _t_ is the variance of the _n_ time points prior to and including time point _t_, but I find that it's best at the exploratory phase to simply discretize the variance into these buckets.
To that end, you get a plot that looks like this, for the moving average date you see above:

<img width="500" height="362" alt="Image" src="https://github.com/user-attachments/assets/4556c697-c6aa-427c-90ae-983840214a49" />

I really need to pick a color gradient in the future where even the palest part of the spectrum is more visible on my plots, but you can see the value of a plot like this: rather than trying to just gauge where the data looks a little wider than usual on a moving average or standard time series plot, you can see the spikes that indicate outsized variance relative to the baseline.
Just a nice plot that I like to use, and here's hoping you can find a good use for it as well!

Here's the code for that plot:

```r
# 1. Aggregate daily variances
  df2 <-  df %>%
          mutate(pred_date = as.Date(PREDICTION_DATETIME)) %>%
          group_by(pred_date) %>%
          mutate(var1 = var(ERR1, na.rm=TRUE),
                 var2 = var(ERR2, na.rm=TRUE),
                 var3 = var(ERR3, na.rm=TRUE),
                 var4 = var(ERR4, na.rm=TRUE),
                 var5 = var(ERR5, na.rm=TRUE),
                 var6 = var(ERR6, na.rm=TRUE),
                 var7 = var(ERR7, na.rm=TRUE)) %>%
          select(pred_date, var1, var2, var3, var4, var5, var6, var7) %>% 
          unique() %>% 
          filter(  !is.na(var1) || 
                   !is.na(var2) || 
                   !is.na(var3) || 
                   !is.na(var4) || 
                   !is.na(var5) || 
                   !is.na(var6) || 
                   !is.na(var7) )
  
#2. Plot the variances
  
  # 2.1. reshape data from wide to long format
  df2_long <- melt(df2, id.vars = "pred_date") %>%
    filter(variable %in% c('var1', 'var2', 'var3', 'var4', 'var5', 'var6', 'var7'))

  
  # 2.2. plot the variances
  ggplot(df2_long, 
         aes(x=pred_date, y=value, color=variable)) +
    geom_line(size=1) +
    scale_colour_manual(values = c('lightcyan', 'lightblue1','lightblue', 'lightskyblue','steelblue1', 'royalblue1', 'navy')) + 
    labs(title = "Error Averages (8-term Moving Averages)", x = "Date", y = "Moving Average") +
    #theme_fivethirtyeight() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
```
