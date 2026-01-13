INSERT INTO production.fact_pa(
    game_pk, pitcher_id, batter_id, game_counter,
    last_pitch_number, pitcher_pa_number, times_through_order,
    balls, strikes, outs_when_up, inning, inning_topbot,
    events, bat_score, fld_score, post_bat_score,
    bat_score_diff
)
SELECT 
    ab.game_pk,
    ab.pitcher AS pitcher_id,
    ab.batter AS batter_id,
    ab.game_counter,
    ab.last_pitch_number,
    ab.pitcher_pa_number,
    ab.times_through_order,
    ab.balls,
    ab.strikes,
    ab.outs_when_up,
    ab.inning,
    ab.inning_topbot,
    ab.events,
    ab.bat_score,
    ab.fld_score,
    ab.post_bat_score,
    ab.bat_score_diff
FROM staging.statcast_at_bats ab
ON CONFLICT (game_pk, game_counter) DO NOTHING;

INSERT INTO production.fact_pitch (
    pa_id, game_pk, pitcher_id, batter_id, game_counter, pitch_number,
    pitch_type, pitch_name, description, release_speed, effective_speed,
    release_spin_rate, release_extension, spin_axis, pfx_x, pfx_z,
    zone, plate_x, plate_z, balls, strikes, outs_when_up, bat_score_diff,
    is_whiff, is_called_strike, is_bip, is_swing, is_foul
)
SELECT
    pa.pa_id,
    p.game_pk,
    p.pitcher AS pitcher_id,
    p.batter AS batter_id,
    p.game_counter,
    p.pitch_number,
    p.pitch_type,
    p.pitch_name,
    p.description,
    p.release_speed,
    p.effective_speed,
    p.release_spin_rate,
    p.release_extension,
    p.spin_axis,
    p.pfx_x,
    p.pfx_z,
    p.zone,
    p.plate_x,
    p.plate_z,
    p.balls,
    p.strikes,
    p.outs_when_up,
    p.bat_score_diff,
    p.is_whiff,
    p.is_called_strike,
    p.is_bip,
    p.is_swing,
    p.is_foul
FROM staging.statcast_pitches p
JOIN production.fact_pa pa
    ON pa.game_pk = p.game_pk
    AND pa.game_counter = p.game_counter
ON CONFLICT (game_pk, game_counter, pitch_number) DO NOTHING;