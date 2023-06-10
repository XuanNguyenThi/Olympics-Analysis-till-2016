-- Databricks notebook source
-- MAGIC %md ###NGUYEN THI XUAN
-- MAGIC ** SPORT OLYMPICS **
-- MAGIC
-- MAGIC - Sport is alaways an essential part of our lives. To a country, not only economics indicators, but also sport rankings shows us verstile strength of a country. Through stages of history time, I would like to uncover and analyse strong points of each country
-- MAGIC - As a badminton player/lover, I would like to deep dive into the top three player who have been creating legendary history of badmiton
-- MAGIC
-- MAGIC
-- MAGIC - I downloaded datasets from the years from 1896 to 2016 at Kaggle:
-- MAGIC - Click here: https://www.kaggle.com/datasets/heesoo37/120-years-of-olympic-history-athletes-and-results?resource=download
-- MAGIC - I viewed data on CSV file and replace correct place for "NA", format cells and then uploaded the dataset in my cluster on Databricks and started to check data and to analyse the analysis
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ###QUESTIONS TO ANSWER
-- MAGIC 1. Speaking of economy, UK and China are the best ranking with high GDP, so what about in sports?
-- MAGIC 2. More men joins in the game than women?
-- MAGIC 3. The younger the atheletes are, the stronger and medals they get
-- MAGIC 4. Who won the most medals in badmiton? and which country seems developing in badmiton?
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ### HYPOTHESIS
-- MAGIC 1. Yes, UK and China wins more medals in most game. More, the Asians are small, flexible, clever than Europes in some sport related to running/skills/balls. Europes and Americas beats Asias in sports related to strength, strategy
-- MAGIC 2. Yes, we can check the ratio between men and women but it's clear to see that man is proactive
-- MAGIC 3. Yes, the younger earns more medals than the older/experiencers, but it depends on what kind of sports
-- MAGIC 4. As a badmiton player and following badminton game tours, I believe that China is the strongest candidate in badmintion. However, I can't deny that Korea and Japan will be the next and are great competitors

-- COMMAND ----------

-- MAGIC %md ###                                   DATA ANALYSIS
-- MAGIC **I decided to go for the metrics that interested me the most: Medal, Sport, Year, Sex, Region**
-- MAGIC **I also find ways to filter which country get the best medals in each sport** **and I use Growth Rate to calculate the growth trend of China country
-- MAGIC
-- MAGIC **First, let's see the general medals as well as the sport for each country throughout the years.**

-- COMMAND ----------

SELECT
  Medal,
  count(Medal) AS Number_of_Medals,
  B.region,
  Year,
  Sport
FROM
  default.athlete_events_1_csv AS A
  LEFT JOIN default.noc_data_csv AS B ON A.NOC = B.NOC
WHERE
  Medal <> 'NA'
GROUP BY
  Medal,
  B.region,
  Year,
  Sport

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![country](files/tables/country.png)
-- MAGIC
-- MAGIC **As we can see UK owns the most nearly 50% of all medals in Olympics**

-- COMMAND ----------

SELECT DISTINCT C.Sport,
       max(C.medals) OVER (PARTITION BY C.Sport) AS max_medals,
       FIRST_VALUE(C.Region) OVER (PARTITION BY C.Sport ORDER BY C.medals DESC) As best_region
FROM (
    SELECT
      count(Medal) AS medals,
      Sport,
      B.region
    FROM
      default.athlete_events_4_csv AS A
      LEFT JOIN default.noc_data_3_csv AS B ON A.NOC = B.NOC
    WHERE
      Medal <> 'NA'
    GROUP BY Sport, B.region
     ) AS C

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![countrystrength](files/tables/Charts_of_strong_countries.png)
-- MAGIC
-- MAGIC **We'd like to see which sport each country are good at. For examples: China is good at Badminton, Table Tennis and Trampolining.**
-- MAGIC
-- MAGIC **As Vietnamese person, I can see that Vietnam didn't get an first ranking in any sport.**
-- MAGIC
-- MAGIC **But to compare in our own sport, we are better in Shooting and Taekwondo**

-- COMMAND ----------

    SELECT
      count(Medal) AS medals,
      Sport,
      B.region
    FROM
      default.athlete_events_4_csv AS A
      LEFT JOIN default.noc_data_3_csv AS B ON A.NOC = B.NOC
    WHERE
      Medal <> 'NA'
    AND B.region = 'Vietnam'
    GROUP BY Sport, B.region

-- COMMAND ----------

-- MAGIC %md  
-- MAGIC ####Next, we will see the trends of gender, ages who took participating in sports. 

-- COMMAND ----------

SELECT
  Medal,
  count(Medal) AS Number_of_Medals,
  Sex,
  Year,
  B.region,
  Sport
FROM
  default.athlete_events_1_csv AS A
  LEFT JOIN default.noc_data_csv AS B ON A.NOC = B.NOC
WHERE
  Medal <> 'NA'
GROUP BY
  Medal,
  Sex,
  Year,
  B.region,
  Sport

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![chart](files/tables/sex-1.png)
-- MAGIC
-- MAGIC **We can see it clear that the gaps betweens men and women becomes smaller. It's said that from 2000, women is treated equally and earns so many medas as men. This is a big milestone to women specifically and to sport world in generally**
-- MAGIC

-- COMMAND ----------

SELECT
  Medal,
  count(Medal) AS Number_of_Medals,
  Sex,
  Year,
  B.region,
  Sport,
  (
    CASE
      WHEN Age < 20 THEN '< under 20'
      WHEN Age BETWEEN 21
      AND 30 THEN 'between 20-30'
      WHEN Age > 30 THEN 'over 30'
      ELSE 'Unknown'
    END
  ) AS Age_Group
FROM
  default.athlete_events_1_csv AS A
  LEFT JOIN default.noc_data_csv AS B ON A.NOC = B.NOC
WHERE
  Medal <> 'NA'
GROUP BY
  Medal,
  Sex,
  Year,
  B.region,
  Sport,
  Age_Group
HAVING
  Age_Group <> 'Unknown'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![age_group](files/tables/age.png)
-- MAGIC
-- MAGIC
-- MAGIC **I would like to divide age into 3 groups: under 20, between 20-30, and over 30 years old.**
-- MAGIC
-- MAGIC **We can see that the middle ages occuppied the most medals. Under 20 and over 30 years old comes in 2nd place with less medals in most sports.**
-- MAGIC
-- MAGIC **However, we can see the exception in 'Shooting, Sailing and Equestrianism', over 30 years-old players have strong point in experiences and achieved more medals thant 20-30 years-old**

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ####Due to my passion to badmintion, I'd like to see which country is becoming better nowadays, and which player scored most medals
-- MAGIC
-- MAGIC **Gao Ling from China wins the most medal during her Olympics time. What a surprise!**
-- MAGIC
-- MAGIC

-- COMMAND ----------

SELECT
  count(Medal) AS Number_of_Medals,
  A.ID,
  A.Name
FROM
  default.athlete_events_3_csv AS A
  LEFT JOIN default.noc_data_2_csv AS B ON A.NOC = B.NOC
WHERE
  Medal <> 'NA'
  AND Sport = 'Badminton'
GROUP BY
  A.ID,
  A.Name
ORDER BY
  Number_of_Medals DESC
LIMIT
  3

-- COMMAND ----------

SELECT
  Medal,
  count(Medal) AS Number_of_Medals,
  B.region,
  Year,
  Sport
FROM
  default.athlete_events_1_csv AS A
  LEFT JOIN default.noc_data_csv AS B ON A.NOC = B.NOC
WHERE
  Medal <> 'NA'
  AND Sport = 'Badminton'
GROUP BY
  Medal,
  B.region,
  Year,
  Sport

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![badminton_table](files/tables/badminton_medals.png)
-- MAGIC
-- MAGIC
-- MAGIC **It's crytal clear to say China is the NUMBER ONE in badminton with the most medals**

-- COMMAND ----------

SELECT
  Call.region,
  Year,
  Number_of_Medals,
  (
    Number_of_Medals - LAG (Number_of_Medals) OVER (
      ORDER BY
        Year ASC
    )
  ) / LAG (Number_of_Medals) OVER (
    ORDER BY
      Year ASC
  ) * 100 AS percentage_growth
FROM
  (
    SELECT
      count(Medal) AS Number_of_Medals,
      B.region,
      Year
    FROM
      default.athlete_events_3_csv AS A
      LEFT JOIN default.noc_data_2_csv AS B ON A.NOC = B.NOC
    WHERE
      Medal <> 'NA'
      AND Sport = 'Badminton'
      AND B.region = 'China'
    GROUP BY
      B.region,
      Year
    ORDER BY
      B.region,
      Year ASC
  ) AS Call

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![china_growth](files/tables/china_badminton_growth.png)
-- MAGIC
-- MAGIC
-- MAGIC **I would like to spend metrics GROWTH RATE for this tasks**
-- MAGIC
-- MAGIC **We can see that China is still the leading company when it comes to Badminton, but let's see if it can protect their rankings during all these decades**
-- MAGIC
-- MAGIC **From 1995 till 2000, 2010, 2015, China got so many medals, however, it started to reduce from 2016 with the new champion Malaysia and Japan**
-- MAGIC
