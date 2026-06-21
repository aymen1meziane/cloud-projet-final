-- ============================================================================
-- Gold — Datamarts NYC Mobility (RNCP36739)
-- ============================================================================
-- Source : table Silver `nyc_mobility_gold.trips` (1 ligne = 1 course enrichie).
-- Exécution : Amazon Athena, base `nyc_mobility_gold`.
-- Chaque datamart répond à une problématique métier et est matérialisé en
-- Parquet sur S3 via CREATE TABLE AS SELECT (CTAS).
--
-- Pour ré-exécuter un datamart, supprimer d'abord la table ET son dossier S3 :
--   DROP TABLE nyc_mobility_gold.gold_xxx;
--   (puis vider s3://nyc-mobility-silver-am2024/gold/xxx/ avant un nouveau CTAS)
-- ============================================================================


-- ----------------------------------------------------------------------------
-- DATAMART 1 — Rentabilité chauffeur  (Problématique 1)
-- Quels créneaux / zones / types de taxi rapportent le plus ?
-- Grain : borough × zone × type de taxi × heure de la journée.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_rentabilite
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-silver-am2024/gold/rentabilite/'
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
FROM trips
WHERE pickup_borough <> 'Unknown'
GROUP BY pickup_borough, pickup_zone, taxi_type, hour(pickup_hour);

-- Insight : aéroports (JFK puis LaGuardia, Queens) dominent ; pic 14h-22h ;
-- uniquement des Yellow (règle TLC : exclusivité Manhattan/aéroports).


-- ----------------------------------------------------------------------------
-- DATAMART 2 — Impact météo & 311 sur la demande  (Problématique 2)
-- La météo et les signalements 311 influencent-ils la demande / le revenu ?
-- Grain : borough × tranche de pluie × tranche de température.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_meteo_demande
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-silver-am2024/gold/meteo_demande/'
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
FROM trips
WHERE pickup_borough <> 'Unknown'
GROUP BY
    pickup_borough,
    CASE WHEN precipitation = 0 THEN '1_sec'
         WHEN precipitation < 2.5 THEN '2_pluie_faible'
         ELSE '3_pluie_forte' END,
    CASE WHEN temperature_2m < 0 THEN '1_gel'
         WHEN temperature_2m < 10 THEN '2_froid'
         ELSE '3_doux' END;

-- Insight : la pluie forte baisse légèrement le panier moyen ; la température
-- a peu d'effet ; les signalements 311 montent par intempéries.
-- NB : pour conclure sur la demande, raisonner en courses/heure (plus d'heures
-- sèches que pluvieuses sur la période).


-- ----------------------------------------------------------------------------
-- DATAMART 3 — Détection d'anomalies tarifaires  (Problématique 3, bonus)
-- Méthode : z-score du tarif, segmenté par trajet (zone départ -> zone arrivée).
-- Une course est anormale si |z-score| > 3 (plus de 3 écarts-types de la
-- moyenne du trajet). On ne juge que les trajets faits >= 30 fois.
-- ----------------------------------------------------------------------------
CREATE TABLE nyc_mobility_gold.gold_anomalies
WITH (
    format = 'PARQUET',
    external_location = 's3://nyc-mobility-silver-am2024/gold/anomalies/'
) AS
WITH stats AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        AVG(total_amount)    AS moy_tarif,
        STDDEV(total_amount) AS ecart_type_tarif,
        COUNT(*)             AS nb_trajets
    FROM trips
    WHERE pickup_borough <> 'Unknown'
      AND total_amount > 0
    GROUP BY pickup_zone, dropoff_zone
    HAVING COUNT(*) >= 30           -- trajets assez fréquents pour une stat fiable
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
FROM trips t
JOIN stats s
    ON  t.pickup_zone = s.pickup_zone
    AND t.dropoff_zone = s.dropoff_zone
WHERE t.total_amount > 0
  AND s.ecart_type_tarif > 0
  AND ABS((t.total_amount - s.moy_tarif) / s.ecart_type_tarif) > 3;

-- Insight : isole des tarifs aberrants sur trajets courts (ex. 9792 $ pour un
-- trajet à 15 $ de moyenne, z-score 85) -> erreurs de facturation / fraude.
-- Limite : seuil +3σ -> ne ressortent que des tarifs trop hauts (plancher à 0).
-- À comparer avec l'Isolation Forest (bonus Bloc 4) = 2 méthodes de détection.


-- ----------------------------------------------------------------------------
-- Vérifications rapides
-- ----------------------------------------------------------------------------
-- SELECT * FROM gold_rentabilite   ORDER BY revenu_total DESC LIMIT 10;
-- SELECT * FROM gold_meteo_demande ORDER BY pickup_borough, tranche_pluie, tranche_temp;
-- SELECT * FROM gold_anomalies     ORDER BY ABS(z_score) DESC LIMIT 20;