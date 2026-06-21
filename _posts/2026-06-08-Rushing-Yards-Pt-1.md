---
layout: post
title: "Rushing Yardage By Time - Part 1"
description: "Do Big XII Offenses Run Better in Night Games"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## Rushing By Time of Day?

### Background

I haven't had a chance to blog for a minute now, but I've finally got a minute to do a little breakdown on something I've long suspected as a college football quasi-fan[^1]: teams run the ball for more yardage in night games. There are obvious reasons you could imagine that this would be the case: it's cooler and the sun isn't out, so running backs tire out more slowly, or maybe just that night games tend to be louder and have more hyped-up audiences, so running backs feed off that and find better physicality, or a half-dozen other potential reasons. For whatever reason, it's just an impression I have. So, now that I have a little bit of time, I thought I'd test that theory.

### First Steps:

Naturally, the first thing to do is pull down some data and throw together some plots. I figured I might as well start with one conference, and one season, since it's a bit of a pain to pull down and aggregate the data (although I really can't say enough about how great www.sports-reference.com is for this kind of data aggregation!), so I'm starting with the Big XII conference, for obvious reasons, and I'll start with the 2025 season. Not the most fun season as a Baylor fan, but c'est la vie.
We'll start with total rushing yardage, but the first thing to acknowledge is that not all teams are equal when it comes to rushing the ball[^2]. Normally sports reporting and metric comparison folks just amortize the total yardage in a given game/season to yards per carry (YPC or Y/C, for shorthand) for a measure of rushing efficiency, which also has issues from a statistical perspective[^3], but that still doesn't account for the fact that some teams are just plain better at running the ball than others. So for my purposes today, I'm going to fully standardize rushing to Z-scores (I know, don't get me started about presuming normality and the small sample sizes of a college football season, but Z-scores are still a great shorthand for graphical analysis). While it would be awesome to pull in something like a metric for the opponent's rushing defense[^4], that might be something I leave for another day.
After we make that decision on our Y-variable, it's time to consider what qualifies as a "night game". For this purpose, I'm simply going to pull in the time of kickoff, and call anything starting at or after 6:00 PM (local time) a night game, although I'm fully aware how early night comes for those November games in Ames.

### Basics

With those considerations in might, Let's start with a couple basic plots. You can always go crazy doing plots for your EDA, so I figure I'll start easy here. Here's what the total yardage split looks like, and the Z-scores for the total yardages:

<img width="1563" height="735" alt="Image" src="https://github.com/user-attachments/assets/b10663ac-3cce-452d-bf8b-1d24c6b8fdc2" />

Nothing too terribly informative about that. I'm not seeing any particular trends between the day and night rushing totals, either in totals or in the Z-scores, but I was absolutely fascinated by those three points at the top, in the 400+ territory. Looks like one day game and two night games, and those are:
  1. Kansas State's 472 rushing yards on the road at Utah - Utah had to come back and win this game 51-47 in the fourth quarter, and it looks like a barnburner.
  2. BYU's 468 yards at home against Portland State - There's not much to say about this one, BYU just racked up stats against a bad FCS team.
  3. Utah's 422 yards at home against Colorado - Utah had the highest rushing average per game in the conference for a reason.

With no notable trends showing up here, this is a good time to move on to yards per carry:
<img width="838" height="735" alt="Image" src="https://github.com/user-attachments/assets/11c7f6d9-9db7-41b1-8553-0392405ea998" />

There *might* be a slight trend here where YPC positively trends with time, but it's not strong, and the variance is quite large. This is about when I start thinking about some kind of transform to suppress the potentially excessive variance, but that'll depend on how the model diagnostics look like. If we just throw a linear regression with a CI on this plot, with no regard for using robust estimators or checking those model diagnostics (which we can do as a first pass, since we're not making inferences), here's what that line looks like:
<img width="478" height="314" alt="Image" src="https://github.com/user-attachments/assets/8cc48a01-f891-4adf-b4df-358ff73855d6" />

The improvement based on time is pretty weak, just 0.089 YPC per hour. What happens if we 









[^1]: I was never much of a college football fan, despite going to college back when my undergrad had one of the best offenses in the nation every year, but I picked up college football from my grandmother. She was a lifelong fan of the University of Texas Longhorns, and so I habitually root for UT as well, despite attending one of UT's two arch-rivals for my graduate school work (gig 'em!). 
[^2]: My favorite example of this is the [2023 UTSA-UNT game](https://www.espn.com/college-football/matchup/_/gameId/401531399), which was painful to watch as a UNT fan, but it was incredible to watch as a general fan of Jeff Traylor's offenses at UTSA.
[^3]: Averaging is easily the most common measure of central tendency, but it's very sensitive to outliers, which are common in measuring yardage from football (see: the entire study of explosive plays). While there are alternative measures of averaging that fix that make the mean more robust to outliers (I'm particularly partial to the trimean and light winsorizing), the median is computationally easier in most cases, and doesn't require explaining a new measure to non-technical or less-technical audiences.
[^4]: Maybe their efficiency, because an ordinal ranking for just their rushing defense can have problems as a covariate unless you handle the ordinal measures in a specific way)
[^5]: 
[^6]: 
[^7]: 
[^8]: 
[^9]: 
[^10]: 








