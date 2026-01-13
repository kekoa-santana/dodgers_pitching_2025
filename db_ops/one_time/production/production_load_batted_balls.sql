INSERT INTO production.sat_batted_balls (
    pitch_id, bb_type, events, launch_speed, launch_angle, 
    hit_distance_sc, hc_x, hc_y, is_homerun
)
SELECT
    p.pitch_id,
    bb.bb_type,
    bb.events,
    bb.launch_speed,
    bb.launch_angle,
    bb.hit_distance_sc,
    bb.hc_x,
    bb.hc_y,
    bb.is_homerun
FROM staging.statcast_batted_balls bb
JOIN production.fact_pitch p
    ON bb.game_pk = p.game_pk
    AND bb.pitch_number = p.pitch_number
    AND bb.game_counter = p.game_counter
ON CONFLICT (pitch_id) DO UPDATE 
SET bb_type = EXCLUDED.bb_type,
    events = EXCLUDED.events,
    launch_speed = EXCLUDED.launch_speed,
    launch_angle =  EXCLUDED.launch_angle,
    hit_distance_sc =  EXCLUDED.hit_distance_sc,
    hc_x = EXCLUDED.hc_x,
    hc_y = EXCLUDED.hc_y,
    is_homerun = EXCLUDED.is_homerun;