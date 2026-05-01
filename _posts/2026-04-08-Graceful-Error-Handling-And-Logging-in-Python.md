---
layout: post
title: "Gracefully Integrating Error Handling and Logging in Python"
description: "Logging to a custom table and error handling can both be tricky, but they actually go well together."
tags:
  - Blogging
  - Python
---

## Back to The Weather Data

Revisiting my previous posts about scraping weather data from the Open-Meteo API, I recently discovered that they also provide historical temps at a provided latitute/longitude, which is perfect for my error analysis. I was previously planning to just use the day-of predictions as a proxy for true temps, but this allows us to instead both get an accurate error for each time period and to use that day-of prediction for a error observation on each prediction point. Every statistician's favorite phrase: "We got more data!"
So my first approach was just to replicate my process for the original script. Pretty easy set of steps:
    1. Create a WeatherHistoricals table to parallel my existing WeatherPredictions table (since the structure works well for both situations),
    2. Replicate the WeatherPredictions script,
    3. Change the API endpoint target and parameters in the new script to hit the Open-Meteo weather archive endpoint,
    4. Add the new script to my Cron schedule,
    5. Kick back and watch it go.

But I thought, wouldn't this be a good time to add some error-handling for any times there's an API bounceback? That has become a semi-regular occurrence with the Open-Meteo predictions API, but it's a free application and I'm not about to complain about a free API. That just means that it's on me to add some error-handling to handle errors in the logic, log the error in the logic, and then to try again after a waiting period.
You're probably already thinking, "Why would you log the error from the logic rather than the error returned from the HTTPS request to the API?" Well, I tried that initially, and it turns out that Open-Meteo's error responses are fairly nonstandard, and even more than that, they're uninformative. Most of the time, the message body is "none". So we take what we can get from the logic, and usually it'll be an issue with trying to parse the JSON load into a dataframe, I'm guessing.

### Standard Error-Handling Format in Python

If you're fairly accustomed to python, you've probably seen this very standard block:

```python
try:
    <do things>
except Exception as e:
    print(e)
```

This is Python's native try-except block, and it's probably worth noting that there's a further extension that uses the "finally" keyword, but that's mostly for just ensuring that your resources like database connections and file handles close properly. This try-except block does just what it says on the tin: it tries to execute the code in your indented block after the "try" keyword, then if there's an error raised in the execution of that block, the "except" keyword catches that exception and runs the code in the indented block after the "except". You can just use "except:" without the Exception object (note the capital "E" here), but the full pattern above will store your exception in the "e" variable and allow you to interact with it in the body of your except block.

### Logging

Logging is a one of those technical topics that so many different professions in the IT world have to interact with that it can invite a bit of a holy war if you take too firm a position on the "right" way to do it, so I'm going to stick with an extremely basic form of logging here: time-event logging. Time-event logging is exactly what it sounds like, you log the time of the event, a short message about what occurred, and not much else. Usually the structures affected are noted in the message. If you were a CS or MIS major in college, you've probably heard of time-event logging, it's what most of what Microsoft and Linux system logs looked like up until Windows 8 was released in 2012, and most Linux logs are still time-event logs. Microsoft first started experimenting with more verbose logging in SSIS in the early 2000s, and then expanded their use of that format to system logs.
So, back to the task at hand. We're going to need a table to log our results into, so we'll create this table in our coredb database:

```SQL
CREATE TABLE coredb.log (
event_time DATETIME,
message VARCHAR(75)
);
```
You can alter your variable lengths and names as you see fit, but I like to keep it simple.

### Logging From Python

If you check back into my last post about scraping predictions data, you'll remember that this is how I was logging to the raw fact data table:
```python
### Create the connection and attempt to insert data
engine = create_engine(f'mysql+pymysql://{USR}:{PWD}@{HOST}:{PORT}/{DB_NAME}')

try:
        df.to_sql(name='WeatherPredictions', con=engine, if_exists='append', index=False) ### We include index=False to keep Python from trying to >
        print("Successful write.")
except Exception as e:
        print(f"An error occurred: {e}")
```

One common pattern here is to set up a connection block and use that to directly execute your logging INSERT statement explicitly, but one advantage of a very narrow logging table is that it's easier to just use that same to_sql() structure to log our events as well. Here's what both of those patterns look like, respectively:
```python
### Create the connection and attempt to insert data
engine = create_engine(f'mysql+pymysql://{USR}:{PWD}@{HOST}:{PORT}/{DB_NAME}')

try:
        df.to_sql(name=TBL_NAME, con=engine, if_exists='append', index=False)

        #### This is logging via the to_sql pattern, you cansee how brief it is.
        logging_df = pd.DataFrame({'insert_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                        'message': 'Predictions write Successful' },
                                        index=[0])
        logging_df.to_sql(name='log',
                        con=engine,
                        if_exists='append',
                        index=False)

except Exception as e:
        print(f"An error occurred: {e}")

        ### This is the pattern for logging with a connection and explictly executing the INSERT query.
        with engine.connect() as con:
                statement = text(f"""INSERT INTO coredb.log (insert_date, message) VALUES ('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', "Predictions extract failed - Error: {e}");""")
                con.execute(statement)
```

You can see why the to_sql() pattern is preferable, the logging is much less verbose, and you have far fewer parentheses and different types of quotation marks floating around to worry about, especially if you're using F-strings to include your exception type in the log message.

## Integrating Error-handling With Retrying and Logging

So, now it's time to tie everything together. I've run through how you do error-handling in python, what a basic logging table looks like, and a couple of ways that you can log into that table in Python, so now it's time to include retrying.
If you're using a full orchestration tool then you may not even want to inclue any retrying in your error-handling, since your orchestration tool will do this for you, but it's worth seeing what a good pattern looks like.
Like you've probably guessed or seen before, we'll start with an execution loop; this is just a loop that you wrap a large majority of the actual action in your program for macro-scale control flow. You set a variable to control that loop's execution condition, and then only flip that variable to the off state once condtions have been met within the body of the program.
```python
if __name__ == '__main__':
        ### Construct the request (can also be done with the requests library, but niquests is the newer, drop-in iteration)
        <configure the parameters for your request here, see last blog post for how to do this>

        retry = 1  #This is the binary indicator for whether the whole body of this program is executin. It's not even making the HTTPS request unless this =1
        retry_count  =0 # This is our control variable to keep this script from looping forever.

        # Both conditions must be true for the loop to run
        while retry == 1 and retry_count < 3: 
                try:
                        resp = niq.get(url, params=params)

                        <data manipulation happens here>

                        ### Create the connection and attempt to insert data
                        engine = create_engine(f'mysql+pymysql://{USR}:{PWD}@{HOST}:{PORT}/{DB_NAME}')
                        
                        try:
                              df.to_sql(name=TBL_NAME, con=engine, if_exists='append', index=False)
                        
                              #### This is logging via the to_sql pattern, you cansee how brief it is.
                              logging_df = pd.DataFrame({'insert_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                              'message': 'Predictions write Successful' },
                                                              index=[0])
                              logging_df.to_sql(name='log',
                                              con=engine,
                                              if_exists='append',
                                              index=False)
                              retry = 0                           #If everything runs according to plan, set that execution loop to 0, the loop won't run again, and the program will exit. 
                        
                        except Exception as e:
                              print(f"An error occurred: {e}")
                        
                              ### This is the pattern for logging with a connection and explictly executing the INSERT query.
                              with engine.connect() as con:
                                      statement = text(f"""INSERT INTO coredb.log (insert_date, message) VALUES ('{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}', "Predictions extract failed - Error: {e}");""")
                                      con.execute(statement)
                              retry_count = retry_count + 1       #In each exception-handling block, increment the retry counter, you don't want to just be on a failure loop forever.
                              time.sleep(900)                     # Because the failure was likely due to an issue with the JSON load returned from the API, give it time to either catch up on the queue or just deal with whatever's keeping the API from promptly returning the data as expected. This is 900 seconds, or 15 minutes.

                        except Exception as e2:
                                print(f"An error occurred: {e2}")
                                retry_count = retry_count + 1
                                time.sleep(900)                 
```

## Wrapping up:

Now that you've seen what graceful error-handling in Python looks like, you might be wondering what it looks like with a more specific error from the HTTP(S) request. Part of the original HTTP protocol was a standardized series of numerical codes that indicate certain responsse conditions. You've probably seen the classic Error 404, which specicially indicates that the website you're trying to access exists, but that particular page on that website couldn't be found. The other relatively common one you may be familiar with is code 200, which is the success code, it just indicates that the page was found and delivered by the web server. The problem is that, over time, various sites and web APIs have begun using nonstandard HTTP repsonse codes that can indicate more specific conditions for their site, but these require that a good web developer was thoughtful enough to set them up and keep their custom library of codes and the corresponding logic maintained. This is perilously uncommon, in my experience, so I usually like to rely on any actual messages that the server is returning alongside that code.
Some sites have incredibly informative messages, like if you ever try to poke one of Google's many servers out there. You can do it just to see what happens, they don't really care about a ping here or there, but I don't recommend trying to DDOS Google. That's a good way to get IP-banned from their services. On the other end of the informativity spectrum, you have services like Open-Meteo, where their HTTP response messages are empty, and you don't get anything to work with to determine whether they've just got a bunch of requests queued or whether it's something more serious, like an outage upstream of their web servers.

Now that you've got that in mind, maybe you want to go play with another free API that's available online. I had a pretty easy time using the [CurrencyBeacon API for a few projects in grad school](https://currencybeacon.com/api-documentation), and I've also heard good things from a friend about [this animal movement tracking API a while ago](https://github.com/movebank/movebank-api-doc).
