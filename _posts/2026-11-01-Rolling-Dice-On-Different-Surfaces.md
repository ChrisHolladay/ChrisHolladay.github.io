---
layout: post
title: "On a D&D Tradition: A Statistical Analysis of the Effects of the Surface on Which Dice are Rolled"
description: "An Analysis of an Old Gaming Superstition"
tags:
  - Blogging
  - Statistics
  - Analysis
---

## On a D&D Tradition: A Statistical Analysis of the Effects of the Surface on Which Dice are Rolled

### Background

If you've explored my repositories and pet projects from past years here on Github, you may have surmised that I've been a fairly active tabletop game player for many years now: I started in Dungeons and Dragons at the tail end of 4e, went to college and was a young adult during the heyday of D&D 5e, and eventually branched out into Call of Cthulu, Monster of the Week, and finally tabletop wargames like Warhammer 40,000.
Similar to any other hobby that relies on chance, the practitioners of tabletop games often have superstitions that they believe will affect their rolls of the dice; these range from the common habits like blowing on dice for luck, which you also see regularly at a craps table, to slightly more esoteric habits like retiring a die for the night after a few bad rolls, to the fairly arcane practices - I once played Warhammer with a great guy who insisted on rolling his dice by flipping them up with his thumb like one would flip a coin. I never quite understood it, but he insisted that it increased the variance of the roll for a fairer outcome. One of the more common and longstanding habits amongst the D&D community is the avoidance of rolling on paper. I've heard this explained in multiple ways, from the simple "It's bad luck" to the more interesting and potentially viable notion that the paper dampens the die's intertia and absorbs more kinetic energy from the die, which mitigates the die's bounce and the amount of rotation. To me, just superficially, this sounds viable, especially if you're rolling on a surface with multiple stacked pieces of paper, like a notepad.
I had a little bit of downtime during a recent game of Monster of the Week that I was playing in, so I decided to test a couple of simply hypotheses: 

1. Rolling on paper is unlucky, compared to rolling on a surface like the tabletop, or a dice tray.
2. There's no difference in dice rolls across these three surfaces.

The experiment runs as such:
- Two D6 dice, which are the standard for Monster of the Week games, will be rolled together and summed for each trial.
- For each surface, n = 40 trials will be conducted.
- The first hypothesis will be tested with a pair of one-sided t-tests, and the second with the F-test from a one-way ANOVA.
- Three tests on the same set of observations obviously invite a multiple testing problem, so I'll default to the quick and dirty Bonferroni correction. This means that we take our base p-value ($\alpha$ = 0.05), and divide it by our number of trials (*m* = 3), to get the new threshold: $p_3$ = 0.166.

After running these trials, much to the confusion of a good friend sitting across the table from me, I came away with the following data, nicely visualized with histograms:

<img width="786" height="735" alt="image" src="https://github.com/user-attachments/assets/6ba1863a-d1cb-4171-bee2-f467c72303f4" />

On first glance, contrary to my initial inclinations, it does actually look like there's greater mass in the left leg of the distribution for paper than for wood (e.g. rolling right on a wooden tabletop) or tray (a special rolling tray, borrowed from the aforementioned confused friend across the table). Whether or not this is a significant difference is for further examination.

### ANOVA

An ANOVA is a tremendously useful first-pass analysis, since it's essentially an extension of the two-sample t-test to more groups, and the basic ANOVA is also quite robust to certain violations of some assumptions, primarily the normality of the residuals. To that end, we still have to look at the assumptions.
We take a couple of the ANOVA assumptions as given, like the independence of the data collected (which we take as independent due to the nature of our trials, although I suppose that you could make an argument that each trial influenced the subsequent trial via the starting position if the dice were picked right up and dropped, but we'll treat this as trivial because the dice were tossed with moderate force to assure a couple of strong bounces), but there are still two assumptions to test:

1. Normality of the Residuals:
   To test this assumption, we first need to _get_ those residuals, so we generate our ANOVA object, extract the residuals, and plot them on the classic normal Q-Q plot (with a reference line, for convenience).

   <img width="925" height="714" alt="image" src="https://github.com/user-attachments/assets/6aff9340-6878-474e-b23c-5f73cd77946e" />

   That's a pretty good plot to me. I'm very content with that.
 
3. Homogeneity of Variances across groups
   This is one of those assumptions that can be just slightly controversial to test. The first-pass approach should always be the graphical assessment:

   <img width="1146" height="723" alt="image" src="https://github.com/user-attachments/assets/9774baf7-a57e-402f-99e1-9073f4f3ae24" />

   Unfortunately, this just isn't terribly informative. What if we look at the three groups' actual variances, compared?

   <img width="190" height="59" alt="image" src="https://github.com/user-attachments/assets/9add2c17-b612-4f51-be75-f2d0c19344f0" />

   It looks like there _might_ be a difference here, but it's hard to tell whether it's sufficient enough of a violation of the assumption that it will interfere with the accuracy of the F-test. To make the conclusion a little more concrete, a good pair of tests here is Bartlett's test

   
