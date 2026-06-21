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

<img width="1563" height="500" alt="Image" src="https://github.com/user-attachments/assets/b10663ac-3cce-452d-bf8b-1d24c6b8fdc2" />

Nothing too terribly informative about that. I'm not seeing any particular trends between the day and night rushing totals, either in totals or in the Z-scores, but I was absolutely fascinated by those three points at the top, in the 400+ territory. Looks like one day game and two night games, and those are:
  1. Kansas State's 472 rushing yards on the road at Utah - Utah had to come back and win this game 51-47 in the fourth quarter, and it looks like a barnburner.
  2. BYU's 468 yards at home against Portland State - There's not much to say about this one, BYU just racked up stats against a bad FCS team.
  3. Utah's 422 yards at home against Colorado - Utah had the highest rushing average per game in the conference for a reason.

With no notable trends showing up here, this is a good time to move on to yards per carry:

<img width="700" height="355" alt="Image" src="https://github.com/user-attachments/assets/d5ed770a-2280-4316-ba2f-49f8e1a79179" />

There *might* be a slight trend here where YPC positively trends with time, but it's not strong, and the variance is quite large. This is about when I start thinking about some kind of transform to suppress the potentially excessive variance, but that'll depend on how the model diagnostics look like. If we just throw a linear regression with a CI on this plot, with no regard for using robust estimators or checking those model diagnostics (which we can do as a first pass, since we're not making inferences), here's what that line looks like:

<img width="478" height="314" alt="Image" src="https://github.com/user-attachments/assets/8cc48a01-f891-4adf-b4df-358ff73855d6" />

The improvement based on time is pretty weak, just 0.089 YPC per hour later that the game kicks off; that means that the expected difference between a game with a noon kickoff and a night game with the classic 6 PM kickoff would be just 0.534 yards per carry; that's not even worth considering. Next up is trying to standardize the variance and mitigate the excess variance, and my favorite default transforms are natural log and square root, so let's take a look at what those do to yards per carry:

<img width="1113" height="558" alt="Image" src="https://github.com/user-attachments/assets/040bee4f-4c27-4731-abb2-61c786b7300b" />

Once again, there's not a particularly distinct trend in either plot. The one thing here that pops out is the one game with a sub-1.00 YPC in the entire data set is TCU's performance against Arizona State[^5], and that's largely because TCU had three things working against them:
  1. TCU was an air raid team, so running the ball was never their forte.
  2. TCU's quarterback, Josh Hoover, is already a rather immobile soul, relative to most modern FBS quarterbacks. His negative rushing yardage in the majority of his games over the last two years speaks for itself.
  3. Arizona State's defensive linemen spent quite a bit of time in TCU's backfield. Six sacks is absolutely brutal, and those count against the QB's rushing yardage for the game. That might explain how Hoover had -38 yards rushing in this game.

There may not be much to say about this model, and before bothering to dig into the diagnostics, the model summary is similarly unpromising. Non-statisticians being taught to use R-squared as a prozy measure of model strength/significance/utility is a perpetual annoyance to statisticians, but it does have value when you recognize that it expresses the amount of variance in the output that's explained by the covariate(s), so while it's not great for setting a level on model strength, it _is_ useful for setting a floor on model strength. All that to say, a very low R-squared indicates that a model's not great for making predictions:

<img width="416" height="176" alt="Image" src="https://github.com/user-attachments/assets/2688913a-71e4-4b24-b699-c036f3286057" />

That model might be statistically significant (the F-test for model significance), and the predictors might even be viable for inference (which you can't assume on-spec, since we haven't done any model diagnostics, and we already have reason to assume that the errors will be non-normal), but it's just a plain ol' bad model for predicting output. On thing we can do, however, is control for the particular school and then instead predict Z-scores[^6] rather than pure yardage.

This is where we have two possible modeling strategies:
  1. We model the kickoff time, and include the school as a simple factor variable. This doesn't really answer the question, because the school is an indicator variable that won't interact with the kickoff time, so it just affects the intercent, and that'll just raise/lower the line for schools that are all-around better at running the ball.
  2. We model this as an interaction model. We can include kickoff time as a solo variable if we want to draw conclusions about all of the teams together, but this modeling approach gives us the opportunity to control for school and see which teams ran the ball better at night than during the day, on their own:
     <img width="569" height="362" alt="Image" src="https://github.com/user-attachments/assets/c519367f-85a8-4997-a4d3-db235bad2d1c" />

This is an interesting model. We'll have to look at the diagnostics to be sure, but this indicates a much stronger model. These hour-by-hour gauges on time are still quite small, what if we fully bucket all of these games into night or day games, and we just want to see how much better each team runs the ball at night versus during the day? This approach naturally runs headlong into some _really_ flagrant small-sample problems, but this is just a quick and dirty effect size estimation. Of note, since we've reduced this to the effect of two factor variables on a continuous variable, this has become a simple two-way ANOVA model.

<img width="530" height="313" alt="Image" src="https://github.com/user-attachments/assets/59eb7db9-3784-469b-a16d-6b95ca38347d" />

This is interesting, because we see that several of the teams with statistically significant effects in the previous model have dropped off in this model's summary: most notably Kansas and Kansas State, while Baylor joined the pack of teams with a statistically significant effect[^7]. I wouldn't put any faith in inferences from this model, due to the absolutely minuscule sample sizes, but it's still a fun little modeling exercise

### Model Diagnostics:

I've been mentioning all throughout this post about needing to do model diagnostics, and I _should_ actually do those, if only for the sake of making it visible and reminding myself how to do them. Let's run through them for the basic model regressing YPC Z-score on kickoff time, with an indicator for night games:

#### Normality of Errors

This one's always the easiest to check, you can extract the residuals from the `lm()` model object and then toss them into `qqnorm()` to get the Q-Q plot for the residuals, and look for reasonable linearity. Unfortunately for us, this doesn't look particularly linear to me:

<img width="868" height="573" alt="Image" src="https://github.com/user-attachments/assets/a1b393a8-e6d6-4e2f-b62d-0fd377038c27" />

This has mixed consequences. I always come back to [this paper](https://www.carlislerainey.com/papers/heavy-tails.pdf) when I have to remind myself of the consequences of non-normal errors in linear regression, and the authors state it pretty clearly[^8]: 

> The assumption of normally distributed errors is necessary only for tests of significance; its violation will have no effect on the estimation of the parameters of the regression model. It is quite fortunate that normality is not required for estimation, because it is often very difficult to defend this assumption in practice.

I suspect this is going to go hand-in-hand with an even bigger assumption violation, heteroskedasticity.

#### Standard Error Variance

This one has a few different ways you can check it. I sometimes default to the first way I learned it, where you just plot your standardized residuals against your predictors, but the weird way to do it that I picked up in grad school is to plot the residuals against the values of the Y-values themselves. Now I tend more toward the latter, partially because R will kick out that plot for you as part of the model diagnostics when you run `plot(model)`, but we'll stick with the former because the interpretation is somewhat easier in passing:

<img width="888" height="637" alt="Image" src="https://github.com/user-attachments/assets/6f73d233-ab04-4479-a8e8-682ba583141e" />

So what happens if we put a line on that plot? Turns out it has both upsides and downsides: the good news is that the slope is vanishingly small, so we can almost conclude that the variance does not trend with the predictor. The downside is that the predictor is incredibly statistically insignificant, but we don't care so much about the significance estimate here as we do just the parameter estimate. This might be enough to be informative in some cases, but just based on the shape of the standardized residuals plot, I'm still slightly suspicious.

<img width="889" height="735" alt="Image" src="https://github.com/user-attachments/assets/ff6ac2c3-86e2-4705-9628-968ea0581cb9" />


### Robust Estimators:

When you have confirmed that your assumptions are violated, you can either pivot your modeling strategy entirely to another model that's robust to the issues you've identified, or you can modify your existing strategy; to that end, we have the terrific HC3 estimators, which are the gold standard for standard error estimators in the case that the you have or suspect heretoskedasticity. So this is what the tests of significance look like when conducted on the basic model, using the HC3 errors:

<img width="406" height="91" alt="Image" src="https://github.com/user-attachments/assets/0415a6a2-934a-4acc-a95b-10028f4d5a7c" />

And then, when we look at the simple interaction model to see which schools run the ball better (or worse) at night than during day games, we get these results:

<img width="551" height="248" alt="Image" src="https://github.com/user-attachments/assets/5b0fa306-7f06-486f-a188-46dae87653e6" />

Things look about the same as the regression summary with the regular error estimators, but with some significance results moderated slightly.

# Final Thoughts:

I think this effect is weaker than I'd anticipated, but still nontrivial. It's nice to get to dig in and work on a model, even a basic one, for a little while, though. I'm a little surprised that UCF and Kansas State were in approximately the same neighborhood on hour-over-hour improvements to their YPC Z-scores as Texas Tech, and I'm curious whether that persists with the simple day/night indicator variable rather than a numeric variable that stretches over the whole day's range of potential kickoffs.

Anyway, fun stuff! If I revisit this in the future, I think I may try to include some other way to account for the strength of the opposing team's rushing defense, and maybe a split by a sample of individual running backs, rather than just the entire teams themselves? Who knows. Have a good day!


[^1]: I was never much of a college football fan, despite going to college back when my undergrad had one of the best offenses in the nation every year, but I picked up college football from my grandmother. She was a lifelong fan of the University of Texas Longhorns, and so I habitually root for UT as well, despite attending one of UT's two arch-rivals for my graduate school work (gig 'em!). 
[^2]: My favorite example of this is the [2023 UTSA-UNT game](https://www.espn.com/college-football/matchup/_/gameId/401531399), which was painful to watch as a UNT fan, but it was incredible to watch as a general fan of Jeff Traylor's offenses at UTSA.
[^3]: Averaging is easily the most common measure of central tendency, but it's very sensitive to outliers, which are common in measuring yardage from football (see: the entire study of explosive plays). While there are alternative measures of averaging that fix that make the mean more robust to outliers (I'm particularly partial to the trimean and light winsorizing), the median is computationally easier in most cases, and doesn't require explaining a new measure to non-technical or less-technical audiences.
[^4]: Maybe their efficiency, because an ordinal ranking for just their rushing defense can have problems as a covariate unless you handle the ordinal measures in a specific way)
[^5]: Somehow, this game was a riveting 24-27 game that required Arizona State to stage an incredible fourth-quarter comeback, despite Arizona State nearly doubling TCU's total yardage (498 vs 269), Arizona State winning the forced turnover battle (3 to 1), and Arizona State dominating TCU in time of possession (35:47 vs 24:13). Everything about this game sounds bizarre, and I probably need to go actually watch a replay of it.
[^6]: The Z-scores will be more useful for our question(s), since we're not looking at gross yardage, we're interested in whether teams run the ball _better_ in the night games than the day games. If the Z-scores are higher for night games, that answers the question.
[^7]: And not in a good way, holy cow. Baylor's YPC dropping by a whopping 2.71 yards for night games is brutal, and second barely ahead of Iowa State's 2.75 weaker YPC at night than during the day for the whole conference. At least Iowa State's effect isn't significant, though (although no significance from a model with sample sizes this small should be taken as gospel).
[^8]: Which is, itself, a quote from a 1985 book on multiple regression in practice, by Bill Berry and Stan Feldman. If you're ever in the market for fascinating research on extremism over the last forty years, I can't recommend Feldman's work enough.
