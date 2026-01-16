CREATE OR REPLACE VIEW production.v_tto (
    pitcher_id,
    times_through_order,
    avg_xwoba_on_bip,
    whiff_rate,
    hard_hit_rate_on_bip,
    hard_hit_rate_per_pitch,
    contact_rate,
    inplay_contact_rate,
    batted_balls,
    pitches_in_order
) AS
WITH pitch_level AS (
    SELECT
        ps.pitch_id,
        ps.pa_id,
        ps.pitcher_id,
        pa.times_through_order,
        bb.xwoba,
        bb.hard_hit,
        ps.is_whiff,
        ps.is_contact,
        ps.is_inplay_contact
    FROM production.v_pitch_shape_features ps
    LEFT JOIN production.sat_batted_balls bb
        ON ps.pitch_id = bb.pitch_id
    LEFT JOIN production.fact_pa pa
        ON ps.pa_id = pa.pa_id
),
balls_in_play AS (
    SELECT
        pitcher_id,
        times_through_order,
        ROUND(
            AVG(NULLIF(xwoba::float8, 'NaN'::float8))::numeric,
            3
        ) AS avg_xwoba_on_bip
    FROM pitch_level 
    WHERE is_inplay_contact IS TRUE
    GROUP BY 1,2
),
agg AS (
    SELECT
        p.pitcher_id,
        p.times_through_order,
        ROUND(AVG(p.is_whiff::int)::numeric, 3) AS whiff_rate,
        ROUND(
            AVG((p.hard_hit::int)) FILTER (WHERE p.is_inplay_contact IS TRUE)::numeric,
            3
            ) AS hard_hit_rate,
        ROUND(AVG(p.is_contact::int)::numeric, 3) AS contact_rate,
        ROUND(AVG(p.is_inplay_contact::int)::numeric, 3) AS inplay_contact_rate,
        COUNT(*) FILTER (WHERE is_inplay_contact IS TRUE)::bigint AS batted_balls,
        COUNT(*)::bigint AS pitches_in_order
    FROM pitch_level p
    GROUP BY 1,2
),
with_per_pitch AS (
    SELECT
        pitcher_id,
        times_through_order,
        ROUND(
            (hard_hit_rate * inplay_contact_rate),
            3
        ) AS hard_hit_rate_per_pitch
    FROM agg
)
SELECT 
    a.pitcher_id,
    a.times_through_order,
    bip.avg_xwoba_on_bip,
    a.whiff_rate,
    a.hard_hit_rate,
    wpp.hard_hit_rate_per_pitch,
    a.contact_rate,
    a.inplay_contact_rate,
    a.batted_balls,
    a.pitches_in_order
FROM agg a
LEFT JOIN balls_in_play bip
    ON a.pitcher_id = bip.pitcher_id
    AND a.times_through_order = bip.times_through_order
LEFT JOIN with_per_pitch wpp
    ON wpp.pitcher_id = a.pitcher_id
    AND wpp.times_through_order = a.times_through_order