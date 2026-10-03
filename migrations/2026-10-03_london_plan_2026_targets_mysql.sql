-- Source: https://www.london.gov.uk/3-delivering-homes-and-neighbourhoods-londoners-need#hn1-increasing-londons-housing-stock-1145736-title
-- Draft London Plan 2026, Table 3.1: total homes over 2027/28–2036/37.
-- OPDC: 11,335 retained in source JSON; no matching sm_update_cleaned row.
-- Rollback: ALTER TABLE sm_update_cleaned DROP COLUMN londonPlan2026Target;
ALTER TABLE sm_update_cleaned ADD COLUMN londonPlan2026Target INT UNSIGNED NULL DEFAULT NULL
  COMMENT 'Draft London Plan 2026 Table 3.1 ten-year total 2027/28-2036/37';
START TRANSACTION;
UPDATE sm_update_cleaned SET londonPlan2026Target = 1716 WHERE `ONS Code` = 'E09000001'; -- City of London
UPDATE sm_update_cleaned SET londonPlan2026Target = 9698 WHERE `ONS Code` = 'E09000002'; -- Barking and Dagenham
UPDATE sm_update_cleaned SET londonPlan2026Target = 32849 WHERE `ONS Code` = 'E09000003'; -- Barnet
UPDATE sm_update_cleaned SET londonPlan2026Target = 6217 WHERE `ONS Code` = 'E09000004'; -- Bexley
UPDATE sm_update_cleaned SET londonPlan2026Target = 25316 WHERE `ONS Code` = 'E09000005'; -- Brent
UPDATE sm_update_cleaned SET londonPlan2026Target = 8283 WHERE `ONS Code` = 'E09000006'; -- Bromley
UPDATE sm_update_cleaned SET londonPlan2026Target = 12085 WHERE `ONS Code` = 'E09000007'; -- Camden
UPDATE sm_update_cleaned SET londonPlan2026Target = 21544 WHERE `ONS Code` = 'E09000008'; -- Croydon
UPDATE sm_update_cleaned SET londonPlan2026Target = 31979 WHERE `ONS Code` = 'E09000009'; -- Ealing
UPDATE sm_update_cleaned SET londonPlan2026Target = 21062 WHERE `ONS Code` = 'E09000010'; -- Enfield
UPDATE sm_update_cleaned SET londonPlan2026Target = 27850 WHERE `ONS Code` = 'E09000011'; -- Greenwich
UPDATE sm_update_cleaned SET londonPlan2026Target = 10582 WHERE `ONS Code` = 'E09000012'; -- Hackney
UPDATE sm_update_cleaned SET londonPlan2026Target = 16859 WHERE `ONS Code` = 'E09000013'; -- Hammersmith and Fulham
UPDATE sm_update_cleaned SET londonPlan2026Target = 18911 WHERE `ONS Code` = 'E09000014'; -- Haringey
UPDATE sm_update_cleaned SET londonPlan2026Target = 12010 WHERE `ONS Code` = 'E09000015'; -- Harrow
UPDATE sm_update_cleaned SET londonPlan2026Target = 21712 WHERE `ONS Code` = 'E09000016'; -- Havering
UPDATE sm_update_cleaned SET londonPlan2026Target = 24030 WHERE `ONS Code` = 'E09000017'; -- Hillingdon
UPDATE sm_update_cleaned SET londonPlan2026Target = 17975 WHERE `ONS Code` = 'E09000018'; -- Hounslow
UPDATE sm_update_cleaned SET londonPlan2026Target = 6768 WHERE `ONS Code` = 'E09000019'; -- Islington
UPDATE sm_update_cleaned SET londonPlan2026Target = 5904 WHERE `ONS Code` = 'E09000020'; -- Kensington and Chelsea
UPDATE sm_update_cleaned SET londonPlan2026Target = 6872 WHERE `ONS Code` = 'E09000021'; -- Kingston upon Thames
UPDATE sm_update_cleaned SET londonPlan2026Target = 16370 WHERE `ONS Code` = 'E09000022'; -- Lambeth
UPDATE sm_update_cleaned SET londonPlan2026Target = 13276 WHERE `ONS Code` = 'E09000023'; -- Lewisham
UPDATE sm_update_cleaned SET londonPlan2026Target = 17455 WHERE `ONS Code` = 'E09000024'; -- Merton
UPDATE sm_update_cleaned SET londonPlan2026Target = 26260 WHERE `ONS Code` = 'E09000025'; -- Newham
UPDATE sm_update_cleaned SET londonPlan2026Target = 16130 WHERE `ONS Code` = 'E09000026'; -- Redbridge
UPDATE sm_update_cleaned SET londonPlan2026Target = 5397 WHERE `ONS Code` = 'E09000027'; -- Richmond upon Thames
UPDATE sm_update_cleaned SET londonPlan2026Target = 27799 WHERE `ONS Code` = 'E09000028'; -- Southwark
UPDATE sm_update_cleaned SET londonPlan2026Target = 4781 WHERE `ONS Code` = 'E09000029'; -- Sutton
UPDATE sm_update_cleaned SET londonPlan2026Target = 24704 WHERE `ONS Code` = 'E09000030'; -- Tower Hamlets
UPDATE sm_update_cleaned SET londonPlan2026Target = 19072 WHERE `ONS Code` = 'E09000031'; -- Waltham Forest
UPDATE sm_update_cleaned SET londonPlan2026Target = 23426 WHERE `ONS Code` = 'E09000032'; -- Wandsworth
UPDATE sm_update_cleaned SET londonPlan2026Target = 12224 WHERE `ONS Code` = 'E09000033'; -- Westminster
COMMIT;
