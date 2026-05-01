
---
layout: post
title: "On Plotting - Time Series Data with Iterations"
description: "What makes a good time series plot where there are overlapping data?"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## Background

I've spent most of my professional and educational careers working with data, and every data professional will tell you that one of the hardest parts of the entire "telling a story" notion that we like to talk about in the industry isn't necessarily getting clean data (because data is never *completely* clean, it's only ever clean *enough*), nor is it getting your data into the right place (this can be tough, but many of us find it so interesting that we go do it professionally as Data Engineers); it's visualizing the data. SalesForce even puts on a pretty impressive Tableau conference every summer out in San Diego just for data viz nerds. I've got to tell you that if you go to Tableau Conference and you scoff like I did when I saw the signs for one of their big evening events being something called "Iron Viz", just trust me and go check it out. Get there ten minutes early, because it's packed, and prepare to have your mind blown by what some of those folks can do on a time crunch.
But I digress, data viz is half of the endgame for most of the data work we do; the other half is more results-oriented: statistical/ML-based inference or some kind of data reporting up to regulatory agencies. That means that, if you want to be effective with how you present your data to tell the story, you need to think about the details. You absolutely *can* just throw data out there on slides or on dashboards, but then you're just shooting yourself in the foot right out of the gate. You want your audience/viewers to focus on the story you're trying to tell them, so that means that you need to reduce the distractions like clashing colors or difficult-to-process formatting. After you reduce the distractions, then it's time to think about actually making changes that accentuate the story you want to tell.

Figuring out how much detail to add to a plot is a real balancing act. Some folks will tell you to just present the bare minimum information and use more plots to each back up a different fact (I like to call this the 538 ethos, from back when 538 was getting started in the early 2010s and they were doing great things in data journalism), while others will tell you that you want to knit together all of your related plots so that you can show related trends together for comparison. I find that going too far to the former is beneficial for academic writing, where anyone who's reading your paper beyond the abstract and the conclusions sections is probably pretty interested in the nitty-gritty details; the latter option tends to get messy and muddy faster than a new hog pen. To that end, I want to talk about some types of plots that I've used in the past and really liked, as well as good practices I picked up from a mentor years ago for cleaning up a plot and stramlining it to tell a story.

## Plot Thoughts

#### 1. Model Performance Comparisons:

This is a plot I made when I was turning my thesis into a journal paper for publication last year, the general gist is that there were four binary classification models that needed to be compared based on their predictive accuracy. The semantics of each model are fascinating, but the basics are that there are four things to show on each plot:
  
  1. a probability threshold (also called a cutoff) that minimizes the model error - this is the vertical, dark gray-green line 
  2. a cutoff that maximizes the diagnostic power - this is the vertical blue line
  3. a plotted line for the model sensitivity/recall (e.g. what proportion of patients were successfully diagnosed early at a given cutoff) - this is the maroon line, with y-axis measurements on the left side
  4. a plotted line for the the mean sqaured eror (MSE) at a given predictive threshold - this is the teal line, with y-axis ticks on the right side


<img width="488" height="338" alt="Image" src="https://github.com/user-attachments/assets/43241cc1-dd03-4c10-95d8-4e8fd12010ee" />

This plot has upsides and downsides:

  1. If you're going to do multiple plots together in a grid to compare trends, you absolutely need to have matching colors and axis scales on each so that your viewers can track sizes for analogous variables. You don't want them to have to repeatedly check your legend to see what color represents the thing they want to compare from plot to plot.
  2. Thresholds - If you're going to do vertical/horizontal bars on a plot to indicate a particular point of significance, those can really crowd the plot. I wish that I'd done those in much lighter colors on this plot, since lighter colors don't intrude on the viewing experience as much.
  3. If you're going to do a choppy plot (often called a "stairstep" plot/function) like this, consider whether a short-term rolling average might look a little better. It wouldn't be great if you're specifically using the plot to find threshold, but you probably should be using plots to find those anyway. A rolling average on a plot like this shows the regions where the accuracy is increasing most swiftly, and would better illustrate the continuous regions of the greatest growth in predictive power.

#### 2. Variable Missingness:

This is another plot I made for a paper over variable selection for diagnosis of a particular disease. I didn't discover them until I got to grad school, but missingness plots are by far my favorite way to examine variables before putting them into a model. Missing data is a huge problem that's often overlooked by poorly-trained Data Scientists, and it'

<img width="534" height="413" alt="Image" src="https://github.com/user-attachments/assets/baee98ee-bc95-473a-a959-2b84babeab3b" />

  1. This is a prime example of how plots don't *need* to be colorful or detailed. You'll see a plot like this in basically every paper that discussed formal model construction, and it's never a plot that you just scroll past when you're evaluating a model's performance (especially relative to potential performance improvements).
  2. When I was putting this together, I just realized that the last variable name on the right is partially cut off, and that's the only variable with substantial missingness. The cut off info is in a table below this plot, but that's still emabarrassing to let that error get out into publication.
  3. Angling your variable names, usually along the horizontal axis, is the best way to pack information in without rotating your label 90 degrees and making it vertical. Just don't do small, vertical labels. 

#### 3. Frequency Comparisons:

This is a plot I made some years ago when I was consulting for a few researchers at the Texas A&M Agrilife service, which does great work on agricultural research for farmers and ranchers in Texas. This paper was on the efficacy of a new vaccine for tick encephalitis in white-tailed deer, which is combined with an anti-parasitic to slow the ticks' feeding or kill them. The variable of concern on this plot was the replete (post-feeding) weight of ticks on three different sets of deer: unvaccinated deer, deer given a lower dose of the vaccine, and deer given a higher dose of the vaccine.

<img width="508" height="382" alt="Image" src="https://github.com/user-attachments/assets/50504172-c61b-4b79-8257-1775553a32ab" />

  1. If you're going to do multiple versions of a univariate plot side-by-side, always make sure to align them so that the axis that the variable is on is the parallel axis. In this case, the variable is the replete weight of the ticks on the X-axis, so the plots are stacked and their X-axes run parallel to each other.
  2. As before, keep your colors matching from plot to plot. It may seem fun to mix things up, but too much contrast distracts the viewer from the actual information in the plot.
  3. Densities vs. histograms - both of these work for presenting a frequency companrison, the density version is just normalized and presented as a continuous curve, but it also works if you just create ranged buckets for a histogram. The issue for a histogram is figuring out how many buckets to use, so I just skip that and do density plots most of the time.

## Plotting Better:

Okay, now what about doing plots well? I've been letting my weather prediction scraper run for a few weeks, so now it's probably a decent time to start looking at the data. My data includes the prediction for the temperature in Dallas at a given time point, taken each day for the seven days running up to the predicted date, so I can use those seven predictions and the true temperature at each time point to get the error residuals for each of those seven predictions. Like I said above, I love a rolling/moving average plot to smooth the curves, so I can create a nice seven-day moving average for each error term to show the average error over a week, looking out *n* many days.
I leave those indexed by a generic time variable *t*, and throw it into R (using GGPlot2, because it makes extending your plots drastically easier than the base R plotting functionality), and this is what I get:

<img width="508" height="382" alt="Image" src="https://github.com/user-attachments/assets/2ad9c968-0af4-4dc7-a369-030b5aee2d76" />

That's kind of a tutti-frutti mess. Only things I can take away from that are from the spikes, and I can barely tell whether the errors are decreasing as the predictions get closer to their prediction time. First things first, those labels need to be fixed. Indexing by *t* tells us nothing about actual dates, which would be nice for all the Dallas denizens who know what the weather was like for those dates, and I already mentioned before that I like angled labels on the horizontal axis, so let's try that:

<img width="508" height="382" alt="Image" src="https://github.com/user-attachments/assets/916cc5fe-9623-4b54-954a-4140943fb93e" />

That's better, but those colors need to be fixed. For plots like this, where the factor distinguishing the lines has a distinct ordering to the factor levels, I like to use a monochromatic scale for the colors, getting darker as the levels progress. It's a bit of a gloomy day out today, so let's try blue:

<img width="508" height="382" alt="Image" src="https://github.com/user-attachments/assets/033999b0-f563-41e8-b2a8-8cac150351f4" />

That's much better. You can see that the errors centralize and stabilize on the darker lines, which tells you that the errors are stabilizing as the predictions get closer to their prediction date. This is what I meant way back at the beginning about the details of a plot working to accentuate the points/trends/story that's being conveyed.
Overall, I'm pretty happy with that plot. But there's a 538 theme for GGPlot2, so what happens if I try that?

<img width="508" height="382" alt="Image" src="https://github.com/user-attachments/assets/863036ec-e384-4b66-999a-082408bab630" />

Eh, maybe not so much with the blues. I suppose this is an example of trying to do a little too much.
