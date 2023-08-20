#!/usr/bin/env python
# coding: utf-8

# What are we going to practice in it:
# 1. `SELECT` statement: Retrieve the names of all countries from the "Country" table.
# 2. `WHERE` statement: Retrieve the names of all leagues from the "League" table for the country with the name 'Spain'.
# 3. `JOIN` statement: Retrieve the match details (match_api_id, home_team_goal, away_team_goal) along with the names of the home team and away team for the matches played in the '2015/2016' season.
# 4. `GROUP BY` statement: Retrieve the total number of goals scored by each team in the "Match" table, grouped by the country and league they belong to.
# 5. `HAVING` statement: Retrieve the average number of goals scored per match for teams that have played at least 10 matches in the "Match" table.
# 6. `ORDER BY` statement: Retrieve the names of players and their heights from the "Player" table, sorted in descending order of height.
# 7. `LIMIT` statement: Retrieve the top 10 teams with the highest number of goals scored in a match from the "Team" table.
# 8. `DISTINCT` keyword: Retrieve the unique seasons from the "Match" table.
# 9. `NULL` value detection:  Retrieve the names of players from the "Player" table whose height is not recorded (NULL).
# 10. Subquery: Retrieve the names of players from the "Player" table who have a higher height than the overall average height of all players.
# 11. `BETWEEN` operator: Retrieve the matches from the "Match" table where the number of goals scored by the home team is between 3 and 5 (inclusive).
# 12. `LIKE` operator: Retrieve the names of teams from the "Team" table whose long name starts with 'FC'.
# 13. `COUNT()` function: Retrieve the number of matches played in each league from the "Match" table.
# 14. `MAX()` function: Retrieve the player name and the highest height from the "Player" table.
# 15. `MIN()` function:  Retrieve the player name and the lowest weight from the "Player" table.
# 16. `SUM()` function: Retrieve the total number of goals scored by each team in the "Match" table.
# 17. `AVG()` function: Retrieve the average weight of players in the "Player" table.
# 18. `IN` operator: Retrieve the names of teams from the "Team" table that have played matches in either '2012/2013' or '2013/2014' seasons.
# 19. `JOIN` with multiple tables: Retrieve all the matches played.
# 20. Complex Query to find the height distribution.

# In[2]:


import numpy as np
import pandas as pd
import sqlite3

import warnings 
warnings.filterwarnings("ignore")


# In[3]:


database = "database.sqlite"

conn = sqlite3.connect(database)


# In[4]:


tables = pd.read_sql("""
SELECT *
FROM sqlite_master
WHERE type='table';
""", conn)

display(tables)


# ### We are ready to query
# 
# ```py
# query = pd.read_sql("""
# // Write your queries here
# """, conn)
# 
# query
# ```

# ### Query 1: Retrieve the names of all countries from the "Country" table.

# In[4]:


query1 = pd.read_sql("""
SELECT name
FROM Country;
""", conn)

query1


# ### Query 2: Retrieve the names of all leagues from the "League" table for the country with the name 'Spain'.

# In[5]:


query2 = pd.read_sql("""
SELECT name
FROM League 
WHERE country_id = (
    SELECT id 
    FROM Country
    WHERE name = "Spain"
);
""", conn)

query2


# ### Query 3: Retrieve the match details (match_api_id, home_team_goal, away_team_goal) along with the names of the home team and away team for the matches played in the '2015/2016' season.

# In[6]:


query3 = pd.read_sql("""
SELECT M.match_api_id, TH.team_long_name AS home_team_name, TA.team_long_name AS away_team_name, M.home_team_goal, M.away_team_goal 
FROM Match M
JOIN Team TH ON M.home_team_api_id = TH.team_api_id
JOIN Team TA ON M.away_team_api_id = TA.team_api_id
WHERE season = '2015/2016';
""", conn)

query3


# ### Query4: Retrieve the total number of goals scored by each team in the "Match" table, grouped by the country and league they belong to.

# In[7]:


query4 = pd.read_sql("""
SELECT C.name AS country_name, L.name AS league_name, T.team_long_name, SUM(M.home_team_goal + M.away_team_goal) AS total_goals
FROM Match M
JOIN Team T ON M.home_team_api_id = T.team_api_id
JOIN League L ON M.league_id = L.id
JOIN Country C ON L.country_id = C.id
GROUP BY C.name, L.name, T.team_long_name;
""", conn)

query4


# ### Query 5: Retrieve the average number of goals scored per match for teams that have played at least 10 matches in the "Match" table.

# In[12]:


query5 = pd.read_sql("""
SELECT team.team_long_name, Count(match.match_api_id) as Match_count, AVG(match.home_team_goal + match.away_team_goal) AS Average_score
FROM team
join Match on team.team_api_id = match.home_team_api_id
GROUP BY team.team_long_name
HAVING (Match.match_api_id)>10;
""", conn)

query5


# ### Query 6: Retrieve the names of players and their heights from the "Player" table, sorted in descending order of height.

# In[9]:


query6 = pd.read_sql("""
SELECT player_name, height
FROM Player
ORDER BY height DESC;
""", conn)

query6


# ### Query 7: Retrieve the top 10 teams with the highest number of goals scored in a match from the "Team" table.

# In[13]:


query7 = pd.read_sql("""
SELECT team.team_long_name, count(match.home_team_goal + match.away_team_goal) as Team_goals
FROM team
JOIN match ON team.team_api_id = match.home_team_api_id
GROUP BY team.team_long_name
ORDER BY Team_goals DESC
LIMIT 10;
""", conn)

query7


# ### Query 8: Retrieve the unique seasons from the "Match" table.

# In[14]:


Query8 = pd.read_sql("""
SELECT DISTINCT season
FROM match
""", conn)

Query8


# ### Query 9: Retrieve the names of players from the "Player" table whose height is not recorded (NULL).

# In[16]:


query9 = pd.read_sql("""
SELECT player_name 
FROM player
Where height IS NULL;
""", conn)

query9


# ### Query 10: Retrieve the names of players from the "Player" table who have a higher height than the overall average height of all players.

# In[17]:


query10 = pd.read_sql("""
SELECT player_name
FROM Player
WHERE height > (
    SELECT AVG(height)
    FROM Player
);
""", conn)

query10


# ### Query 11: Retrieve the matches from the "Match" table where the number of goals scored by the home team is between 3 and 5 (inclusive).

# In[5]:


Query11 = pd.read_sql("""
SELECT player_name 
FROM player
Where height > (
    SELECT AVG(height)
    FROM player
);
""", conn)
Query11


# ### Query 12: Retrieve the names of teams from the "Team" table whose long name starts with 'FC'.

# In[11]:


Query12 = pd.read_sql("""
SELECT team_long_name
FROM team
WHERE team_long_name LIKE 'FC%';
""", conn)
Query12


# ### Query 13: Retrieve the number of matches played in each league from the "Match" table.

# In[21]:


query13 = pd.read_sql("""
SELECT L.name AS league_name, COUNT(*) AS match_count
FROM Match M
JOIN League L ON M.league_id = L.id
GROUP BY L.name;
""", conn)
query13


# ### Query 14: Retrieve the player name and the highest height from the "Player" table.

# In[22]:


Query14 = pd.read_sql("""
SELECT player_name, MAX(height)
FROM player;
""", conn)

Query14


# ### Query 15: Retrieve the player name and the lowest weight from the "Player" table.

# In[23]:


query15 = pd.read_sql("""
SELECT player_name, MIN(weight) AS lowest_weight
FROM Player;
""", conn)
query15


# ### Query 16: Retrieve the total number of goals scored by each team in the "Match" table.

# In[24]:


Query16 = pd.read_sql("""
SELECT team.team_long_name, SUM(match.home_team_goal+match.away_team_goal) AS Total_of_goals_scored
FROM match
JOIN team ON match.home_team_api_id = team.team_api_id
GROUP BY team.team_long_name
ORDER BY Total_of_goals_scored;
""", conn)

Query16


# ### Query17: Retrieve the average weight of players in the "Player" table.

# In[12]:


Query17 = pd.read_sql("""
SELECT AVG(weight)
FROM player;
""", conn)
Query17


# ### Query18: Retrieve the names of teams from the "Team" table that have played matches in either '2012/2013' or '2013/2014' seasons.

# In[26]:


Query18 = pd.read_sql("""
SELECT team_long_name
FROM team
WHERE team_api_id IN (
SELECT home_team_api_id
FROM match 
WHERE Season IN ('2012/2013', '2013/2014')
);
""", conn)
Query18


# ### Query 19: Retrieve all the matches played.

# In[27]:


Query19 = pd.read_sql("""
SELECT Match.id, 
    Country.name AS country_name, 
    League.name AS league_name, 
    season, stage, date,
    HT.team_long_name AS  home_team,
    AT.team_long_name AS away_team,
    home_team_goal, away_team_goal                                        
    FROM Match
    JOIN Country on Country.id = Match.country_id
    JOIN League on League.id = Match.league_id
    LEFT JOIN Team AS HT on HT.team_api_id = Match.home_team_api_id
    LEFT JOIN Team AS AT on AT.team_api_id = Match.away_team_api_id
    ORDER by country_name, date;
""", conn)
Query19


# ### Query20: Complex Query to find the height distribution.

# In[14]:


Query20 = pd.read_sql("""
SELECT CASE
    WHEN ROUND(height)<165 then 165
    WHEN ROUND(height)>195 then 195
    ELSE ROUND(height)
    END AS calculated_height, 
    COUNT(height) AS distribution, 
    (avg(PA_Grouped.averege_overall_rating)) AS averege_overall_rating,
    (avg(PA_Grouped.averege_potential)) AS averege_potential,
    AVG(weight) AS averege_weight 
    FROM PLAYER
    LEFT JOIN (SELECT Player_Attributes.player_api_id, 
    avg(Player_Attributes.overall_rating) AS averege_overall_rating,
    avg(Player_Attributes.potential) AS averege_potential  
    FROM Player_Attributes
    GROUP BY Player_Attributes.player_api_id) 
    AS PA_Grouped ON PLAYER.player_api_id = PA_Grouped.player_api_id
    GROUP BY calculated_height
    ORDER BY calculated_height;""", conn)
Query20


# In[ ]:




