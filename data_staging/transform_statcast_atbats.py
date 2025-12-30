# LOAD PARQUET

# GROUP BY (game_pk, at_bat_number)

"""
FOR EACH PA:
    - final event = last pitch with non-null events
    - record start_balls, start_strikes (first pitch of AB)
    - record pitcher and batter (consistent across AB)
    - record inning, inning_topbot
    - create flags: is_strikeout, is_walk, is_bip
"""

# DROP INVALID ABS

# CAST COLUMNS

# LOAD TO POSTGRES

