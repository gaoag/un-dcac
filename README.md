# Electrification Policy and Pollution in Mexico City

Grand prize winning submission to the United Nations Data for Climate Action Challenge. Worked directly with Sergio Castellanos, Pedro Sanchez, Hector Rincon, Apollo Jain, Alan Xu. In the poster below, I was responsible for the 3rd column (simulating and visualizing effects of certain electrification policies on air pollution).

## What's in the repo:
1. The final poster and report (poster at very bottom).
2. ~~Preprocessed data~~ NDA'd. Sorry! 
3. Some of the Jupyter notebooks and code used. The 'annotated' notebooks describe some of the analysis process used. The other notebooks/code was primarily for processing the original data on an Azure cluster. It's pretty messy/final hour sort of stuff, and it won't work without the original data sources and a EPA MOVESPY installation, so you're better off reading the final poster/report.
4. High level summary (nontechnical)
5. Traffic jokes

## Project Summary
Mexico City is one of the most congested cities in the world. At peak timings, travel times are more than doubled. Approximately 10000 preventable deaths per year are caused by PM2.5 and O3 emissions. This congestion predictably produces greenhouse gas emissions as well, contributing to climate change.

Solutions are difficult. A comprehensive solution would require substantial infrastructure changes (electric charging stations, reconfiguring roads and highways, investments in public transit, etc) along with many other targeted policies (rebates and taxes to promote electric vehicle ownership/discourage gas use, no-drive days, etc). A working solution would need to combine all these things in some yet undetermined way. It's hard!

To even begin thinking about solutions, we need to understand the problem in more specific ways. Right now, though we generally know total emissions figures, we don't really understand where and when emissions are the heaviest. The spatial distribution of emissions is incredibly important for isolating _where_ infrastructure changes need to happen, what public health challenges are faced by certain communities of people, and more. 

This was an attempt to use traffic data from Waze and an EPA tool called MOVES to figure out the spatial distribution of traffic and pollution. Due to the high volume of data (over 700GB of text), substantial preprocessing and simplifications of the MOVES algorithms needed to be done (it would take decades, even on a distributed cluster, for MOVES to process all that data). 

It was a mad scramble to meet the competition deadline, due to these computational constraints and numerous other explorations that didn't pan out. But we did it, and the final poster/report came out alright! 

## Traffic jokes
Traffic is not a joke.


![submitted poster](UN-DCAC_Poster_Submitted.png)
