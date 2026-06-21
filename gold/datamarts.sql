-- ============================================================================
-- Gold — Datamarts NYC Mobility (RNCP36739)
-- ============================================================================
-- Source : table Silver `nyc_mobility_gold.trips` (1 ligne = 1 course enrichie).
-- Exécution : Amazon Athena, base `nyc_mobility_gold`.
-- Stockage : bucket dédié `s3://nyc-mobility-gold-am2024/` (séparation stricte
--            des couches médaillon : bronze / silver / gold = 1 bucket chacun).
-- Chaque datamart répond à une problématique métier et est matérialisé en
-- Parquet via CREATE TABLE AS SELECT (CTAS).
--
-- Pour ré-exécuter un datamart, supprimer d'abord la table ET son dossier S3 :
--   DROP TABLE nyc_mobility_gold.gold_xxx;
--   (puis vider s3://nyc-mobility-gold-am2024/xxx/ avant un nouveau CTAS)
-- ============================================================================


-- ----------------------------------------------------------------------------
-- DATAMART 1 — Rentabilité chauffeur  (Problématique 1)        [8552 lignes]
-- Quels créneaux / zones / types de taxi rapportent le plus ?
-- Grain : borough x zone x type de taxi x heure de la journée.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_rentabilite
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-gold-am2024/rentabilite/'
) AS
SELECT
    pickup_borough,
    pickup_zone,
    taxi_type,
    hour(pickup_hour)            AS heure,
    COUNT(*)                     AS nb_courses,
    ROUND(AVG(total_amount), 2)  AS revenu_moyen,
    ROUND(SUM(total_amount), 2)  AS revenu_total,
    ROUND(AVG(trip_distance), 2) AS distance_moyenne
FROM nyc_mobility_gold.trips
WHERE pickup_borough <> 'Unknown'
GROUP BY pickup_borough, pickup_zone, taxi_type, hour(pickup_hour);

-- Insight : aeroports (JFK puis LaGuardia, Queens) dominent ; pic 14h-22h ;
-- uniquement des Yellow (regle TLC : exclusivite Manhattan/aeroports).


-- ----------------------------------------------------------------------------
-- DATAMART 2 — Impact meteo & 311 sur la demande  (Problematique 2) [54 lignes]
-- La meteo et les signalements 311 influencent-ils la demande / le revenu ?
-- Grain : borough x tranche de pluie x tranche de temperature.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_meteo_demande
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-gold-am2024/meteo_demande/'
) AS
SELECT
    pickup_borough,
    CASE
        WHEN precipitation = 0   THEN '1_sec'
        WHEN precipitation < 2.5 THEN '2_pluie_faible'
        ELSE                          '3_pluie_forte'
    END                               AS tranche_pluie,
    CASE
        WHEN temperature_2m < 0  THEN '1_gel'
        WHEN temperature_2m < 10 THEN '2_froid'
        ELSE                          '3_doux'
    END                               AS tranche_temp,
    COUNT(*)                          AS nb_courses,
    ROUND(AVG(total_amount), 2)       AS revenu_moyen,
    ROUND(AVG(nb_311), 1)             AS moy_signalements_311
FROM nyc_mobility_gold.trips
WHERE pickup_borough <> 'Unknown'
GROUP BY
    pickup_borough,
    CASE WHEN precipitation = 0 THEN '1_sec'
         WHEN precipitation < 2.5 THEN '2_pluie_faible'
         ELSE '3_pluie_forte' END,
    CASE WHEN temperature_2m < 0 THEN '1_gel'
         WHEN temperature_2m < 10 THEN '2_froid'
         ELSE '3_doux' END;

-- Insight : la pluie forte baisse legerement le panier moyen ; la temperature
-- a peu d'effet ; les signalements 311 montent par intemperies.
-- NB : pour conclure sur la demande, raisonner en courses/heure (plus d'heures
-- seches que pluvieuses sur la periode).


-- ----------------------------------------------------------------------------
-- DATAMART 3 — Detection d'anomalies tarifaires  (Problematique 3) [61972 lignes]
-- Methode : z-score du tarif, segmente par trajet (zone depart -> zone arrivee).
-- Une course est anormale si |z-score| > 3 (plus de 3 ecarts-types de la
-- moyenne du trajet). On ne juge que les trajets faits >= 30 fois.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_anomalies
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-gold-am2024/anomalies/'
) AS
WITH stats AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        AVG(total_amount)    AS moy_tarif,
        STDDEV(total_amount) AS ecart_type_tarif,
        COUNT(*)             AS nb_trajets
    FROM nyc_mobility_gold.trips
    WHERE pickup_borough <> 'Unknown'
      AND total_amount > 0
    GROUP BY pickup_zone, dropoff_zone
    HAVING COUNT(*) >= 30           -- trajets assez frequents pour une stat fiable
)
SELECT
    t.taxi_type,
    t.pickup_zone,
    t.dropoff_zone,
    t.pickup_datetime,
    t.trip_distance,
    t.total_amount,
    s.moy_tarif,
    ROUND((t.total_amount - s.moy_tarif) / s.ecart_type_tarif, 2) AS z_score
FROM nyc_mobility_gold.trips t
JOIN stats s
    ON  t.pickup_zone = s.pickup_zone
    AND t.dropoff_zone = s.dropoff_zone
WHERE t.total_amount > 0
  AND s.ecart_type_tarif > 0
  AND ABS((t.total_amount - s.moy_tarif) / s.ecart_type_tarif) > 3;

-- Insight : isole des tarifs aberrants sur trajets courts (ex. 9792 $ pour un
-- trajet a 15 $ de moyenne, z-score 85) -> erreurs de facturation / fraude.
-- Limite : seuil +3sigma -> ne ressortent que des tarifs trop hauts (plancher a 0).
-- A comparer avec l'Isolation Forest (bonus Bloc 4) = 2 methodes de detection.


-- ----------------------------------------------------------------------------
-- Verifications rapides
-- ----------------------------------------------------------------------------
-- SELECT * FROM gold_rentabilite   ORDER BY revenu_total DESC LIMIT 10;
-- SELECT * FROM gold_meteo_demande ORDER BY pickup_borough, tranche_pluie, tranche_temp;
-- SELECT * FROM gold_anomalies     ORDER BY ABS(z_score) DESC LIMIT 20;