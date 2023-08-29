--Task 3/1
CREATE OR REPLACE FUNCTION table_transferred_points()
    RETURNS TABLE (Peer1 VARCHAR, Peer2 VARCHAR, PointsAmount INTEGER) AS $$
WITH p2p AS (SELECT checkingpeer AS nm, checkedpeer AS nm2, sum(pointsamount) AS pa
             FROM transferredpoints
             GROUP BY checkingpeer, checkedpeer),
     t1 AS (SELECT COALESCE(p2p.nm, r.nm2)                 AS Peer1,
                   COALESCE(p2p.nm2, r.nm)                 AS Peer2,
                   COALESCE(p2p.pa, 0) - COALESCE(r.pa, 0) AS PointsAmount
            FROM p2p
                     FULL JOIN p2p r ON p2p.nm = r.nm2 AND p2p.nm2 = r.nm
            WHERE COALESCE(p2p.nm, r.nm2) > COALESCE(p2p.nm2, r.nm))
SELECT * FROM t1 ORDER BY LOWER(Peer1), LOWER(Peer2);
$$ LANGUAGE SQL;

SELECT * FROM table_transferred_points();

--Task 3/2
CREATE OR REPLACE FUNCTION get_passed_3_2()
    RETURNS TABLE(Peer VARCHAR, Task TEXT, XP BIGINT) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
        SELECT checks.Peer, SPLIT_PART(checks.task, '_', 1) AS Task, xpamount AS XP
        FROM checks
                 JOIN verter ON checks.id = verter."Check" AND verter.state = 'Success'
                 JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
                 JOIN xp ON checks.id = xp."Check"
        ORDER BY Peer, XP DESC;
END;
$$;

SELECT * FROM get_passed_3_2();

--Task 3/3
CREATE OR REPLACE FUNCTION peers_not_left_campus_in_date(IN date_in VARCHAR)
    RETURNS TABLE (Peer VARCHAR) AS $$
    WITH t1 AS (SELECT peer
                FROM timetracking
                WHERE "Date" = to_date($1, 'DD.MM.YYYY')
                  AND state = 1),
        t2 AS (SELECT peer
                FROM timetracking
                WHERE "Date" = to_date($1, 'DD.MM.YYYY')
                  AND state = 2)
    SELECT * FROM t1 EXCEPT SELECT * FROM t2;
$$ LANGUAGE SQL;

SELECT * FROM peers_not_left_campus_in_date('27.02.2023');

--Task 3/4
CREATE OR REPLACE PROCEDURE points_changed_3_4(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        SELECT peer, sum(pointsamount) AS PointsChange
        FROM (SELECT checkingpeer AS peer, pointsamount
              FROM transferredpoints
              UNION ALL
              SELECT checkedpeer AS peer, (pointsamount * -1) AS pointsamount
              FROM transferredpoints) foo
        GROUP BY peer
        ORDER BY 2 DESC;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL points_changed_3_4();
    FETCH ALL rc;
COMMIT;

--Task 3/5
CREATE OR REPLACE PROCEDURE peer_points_changed_3_1(rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT Peer1 AS Peer, sum(PointsAmount) AS pc
                    FROM table_transferred_points()
                    GROUP BY Peer1),
             t2 AS (SELECT Peer2 AS Peer, sum(PointsAmount) * -1 AS pc
                    FROM table_transferred_points()
                    GROUP BY Peer2)
        SELECT Peer, sum(pc) AS PointsChange
        FROM (SELECT * FROM t1 UNION ALL SELECT * FROM t2) AS tu
        GROUP BY Peer
        ORDER BY 2 DESC;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL peer_points_changed_3_1();
    FETCH ALL rc;
COMMIT;

--Task 3/6
CREATE OR REPLACE PROCEDURE tasks_frequency_3_6(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH tab AS (SELECT task, "Date", sum(counter) AS res
                     FROM (SELECT task, "Date", 1 AS counter
                           FROM checks) go
                     GROUP BY task, "Date"),
            tt AS (SELECT t1."Date", t1.task
        FROM tab t1
                 LEFT JOIN tab t2
                           ON t2."Date" = t1."Date" AND t1.task NOT LIKE t2.task
        WHERE t2.res IS NULL OR t1.res > t2.res OR t1.res = t2.res
        ORDER BY 1 DESC, 2)
    SELECT to_char("Date", 'DD.MM.YYYY') AS Day, SPLIT_PART(task, '_', 1) AS Task FROM tt;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL tasks_frequency_3_6();
    FETCH ALL rc;
COMMIT;

--Task 3/7
CREATE OR REPLACE PROCEDURE peers_finish_block_with_date(block VARCHAR, rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT title
                    FROM tasks
                    WHERE SPLIT_PART(title, '_', 1) SIMILAR TO block || '[0-9]|' || block || '[0-9][0-9]'
                    ORDER BY 1 DESC
                    LIMIT 1),
             t2 AS (SELECT peer, "Date"
                    FROM checks
                             JOIN t1 t ON checks.task = t.title
                             JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
                             JOIN verter v ON checks.id = v."Check" AND v.state = 'Success'
                    ORDER BY 2 DESC)
        SELECT peer, to_char("Date", 'dd.mm.yyyy') AS Day FROM t2;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL peers_finish_block_with_date('C');
    FETCH ALL rc;
COMMIT;

--Task 3/8
CREATE OR REPLACE PROCEDURE tasks_recommended_3_8(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH tab AS ((SELECT peer1, recommendedpeer, count(recommendedpeer) AS res
                      FROM (WITH fr AS (SELECT friends.peer1, friends.peer2
                                        FROM friends
                                        UNION ALL
                                        SELECT friends.peer2 AS peer1, friends.peer1 AS peer2
                                        FROM friends)
                            SELECT fr.peer1, fr.peer2, recommendations.peer, recommendedpeer
                            FROM fr
                                     JOIN recommendations ON fr.peer2 = recommendations.peer AND
                                                             peer1 NOT LIKE recommendations.recommendedpeer) boo
                      GROUP BY peer1, recommendedpeer))
        SELECT tab.peer1 AS peer, tab.recommendedpeer AS recommendedpeer
        FROM tab
                 LEFT JOIN tab t1 ON t1.peer1 = tab.peer1 AND tab.recommendedpeer NOT LIKE t1.recommendedpeer
        WHERE t1.res ISNULL
           OR tab.res > t1.res
           OR t1.res = tab.res
        ORDER BY LOWER(tab.peer1), LOWER(tab.recommendedpeer);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL tasks_recommended_3_8();
    FETCH ALL rc;
COMMIT;

--Task 3/9
CREATE OR REPLACE PROCEDURE per_cent_of_peers_work_two_blocks(block1 VARCHAR, block2 VARCHAR, rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT DISTINCT peer
                    FROM checks
                    WHERE SPLIT_PART(task, '_', 1) SIMILAR TO block1 || '[0-9]|' || block1 || '[0-9][0-9]'),
             t2 AS (SELECT DISTINCT peer
                    FROM checks
                    WHERE SPLIT_PART(task, '_', 1) SIMILAR TO block2 || '[0-9]|' || block2 || '[0-9][0-9]'),
             t3 AS (SELECT peer FROM t1 INTERSECT SELECT peer FROM t2),
             t4 AS (SELECT nickname FROM peers EXCEPT (SELECT * FROM t2 UNION DISTINCT SELECT * FROM t1))
        SELECT round(coalesce((SELECT count(peer) FROM t1) * 100 / NULLIF(count(nickname)::NUMERIC, 0), 0), 2) AS StartedBlock1,
               round(coalesce((SELECT count(peer) FROM t2) * 100 / NULLIF(count(nickname)::NUMERIC, 0), 0), 2) AS StartedBlock2,
               round(coalesce((SELECT count(peer) FROM t3) * 100 / NULLIF(count(nickname)::NUMERIC, 0), 0), 2) AS StartedBothBlocks,
               round(coalesce((SELECT count(t4.nickname) FROM t4) * 100 / NULLIF(count(nickname)::NUMERIC, 0), 0), 2) AS DidntStartAnyBlock
        FROM peers;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL per_cent_of_peers_work_two_blocks('CPP', 'D');
    FETCH ALL rc;
COMMIT;

--Task 3/10
CREATE OR REPLACE PROCEDURE state_percent_3_10(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (WITH tab1 AS (SELECT nickname, "Date" AS data
                                  FROM (SELECT nickname,
                                               cast(to_char(peers.birthday, 'MM-DD')
                                                   AS VARCHAR) AS "Date"
                                        FROM peers) foo)
                    SELECT peer,
                           tab1.data,
                           cast(to_char("Date", 'MM-DD') AS VARCHAR) AS task_data,
                           checks.id,
                           p.state                                  AS pstat,
                           v.state                                  AS vstat
                    FROM checks
                             JOIN (SELECT p2p."Check", p2p.state FROM p2p) p
                                  ON p."Check" = checks.id AND p.state::TEXT NOT LIKE 'Start'
                             JOIN tab1 ON nickname = checks.peer
                             LEFT JOIN (SELECT verter."Check", verter.state FROM verter) v
                                       ON v."Check" = checks.id AND v.state::TEXT NOT LIKE 'Start'
                    WHERE data = cast(to_char("Date", 'MM-DD') AS VARCHAR))
        SELECT coalesce(round(max(CASE WHEN (statstat = 'Success') THEN res END), 2), 0) AS SuccesfulChecks,
               coalesce(round(max(CASE WHEN (statstat = 'Failure') THEN res END), 2), 0) AS UnsuccesfulChecks
        FROM (SELECT statstat,
                     (cast(count(statstat) AS NUMERIC) * 100 / NULLIF((SELECT COUNT(*) FROM peers), 0)) AS res
              FROM (SELECT (CASE
                                WHEN vstat::TEXT = 'Failure'
                                    THEN 'Failure'
                                ELSE pstat::TEXT
                  END) AS statstat
                    FROM t1) boo
              GROUP BY statstat) goo;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL state_percent_3_10();
    FETCH ALL rc;
COMMIT;

--Task 3/11
CREATE OR REPLACE PROCEDURE peers_finish_one_and_two_but_no_three(block1 VARCHAR, block2 VARCHAR, block3 VARCHAR, rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT peer
                    FROM checks
                             JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
                             JOIN verter v ON checks.id = v."Check" AND v.state = 'Success'
                    WHERE task LIKE $1),
             t2 AS (SELECT peer
                    FROM checks
                             JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
                             JOIN verter v ON checks.id = v."Check" AND v.state = 'Success'
                    WHERE task LIKE $2),
             t3 AS (SELECT peer
                    FROM checks
                             JOIN p2p p ON checks.id = p."Check" AND p.state = 'Success'
                             JOIN verter v ON checks.id = v."Check" AND v.state = 'Success'
                    WHERE task LIKE $3)
        (SELECT * FROM t1 INTERSECT SELECT * FROM t2) EXCEPT SELECT * FROM t3;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL peers_finish_one_and_two_but_no_three('C2_SimpleBashUtils', 'C3_S21_StringPlus', 'CPP1_s21_matrix+');
    FETCH ALL rc;
COMMIT;

--Task 3/12
CREATE OR REPLACE PROCEDURE tasks_count_3_12(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH RECURSIVE rec AS (SELECT title AS task1, 0 AS CountC
                               FROM tasks
                               UNION ALL
                               (SELECT title, CountC + 1
                                FROM rec
                                         JOIN tasks t ON task1 = t.parenttask))
        SELECT SPLIT_PART(Task1, '_', 1) AS Task, max(countc) AS PrevCount
        FROM rec
        GROUP BY task1
        ORDER BY 2 DESC;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL tasks_count_3_12();
    FETCH ALL rc;
COMMIT;

--Task 3/13
CREATE OR REPLACE PROCEDURE lucky_days(amount INTEGER, rc REFCURSOR = 'rc') AS $$
DECLARE
    success_counter INTEGER := 0;
    success_days    DATE[]  := '{}';
    current_day     DATE;
    state_value     VARCHAR;
    row_data        RECORD;
BEGIN
    FOR row_data IN SELECT *
                    FROM (WITH t1 AS (SELECT checks.id, checks."Date", p."Time", 'Success' AS State
                                      FROM checks
                                               JOIN tasks t ON t.title = checks.task
                                               JOIN xp x ON checks.id = x."Check" AND x.xpamount >= t.maxxp * 0.8
                                               JOIN p2p p ON checks.id = p."Check" AND p.state = 'Start'),
                               t2 AS (SELECT checks.id, checks."Date", p."Time", 'Failure' AS State
                                      FROM checks
                                               JOIN p2p p ON checks.id = p."Check" AND p.state = 'Start'
                                      EXCEPT
                                      SELECT id, "Date", "Time", 'Failure' AS State
                                      FROM t1)
                          SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY 2, 3) AS ft
        LOOP
            IF current_day IS NULL THEN
                current_day := row_data."Date";
            ELSE
                IF current_day <> row_data."Date" THEN
                    current_day = row_data."Date";
                    success_counter := 0;
                END IF;
            END IF;
            state_value := row_data.State;
            IF state_value = 'Success' THEN
                success_counter := success_counter + 1;
                IF success_counter = $1 THEN
                    success_days := success_days || current_day;
                END IF;
            ELSE
                success_counter := 0;
            END IF;
        END LOOP;

    CREATE TEMPORARY TABLE temp_table
    (
        value VARCHAR
    ) ON COMMIT DROP;

    INSERT INTO temp_table (value)
    SELECT to_char(unnest(success_days), 'Day');

    OPEN rc FOR
        SELECT DISTINCT value AS LuckyDay FROM temp_table;

END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL lucky_days(2);
    FETCH ALL rc;
COMMIT;

--Task 3/14
CREATE OR REPLACE PROCEDURE max_exp_3_14(IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH tab1 AS (SELECT peer, xp
                      FROM (SELECT peer, sum(xpamount) AS xp
                            FROM xp
                                     JOIN checks c ON xp."Check" = c.id
                            GROUP BY peer) boo)
        SELECT peer, xp
        FROM tab1
        WHERE xp = (SELECT max(xp) FROM tab1);
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL max_exp_3_14();
    FETCH ALL rc;
COMMIT;

--Task 3/15
CREATE OR REPLACE PROCEDURE ahead_of_time(entry_time TIME, amount INTEGER, rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT peer, count(peer) AS count_state
                    FROM timetracking
                    WHERE "Time" < entry_time
                      AND state = 1
                    GROUP BY 1)
        SELECT peer
        FROM t1
        WHERE count_state >= amount
        ORDER BY LOWER(peer);
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL ahead_of_time('12:00:00', 3);
    FETCH ALL rc;
COMMIT;

--Task 3/16
CREATE OR REPLACE PROCEDURE not_less_times_last_days_exp_3_14(IN N_DAYS BIGINT, IN M_TIMES BIGINT, IN rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH tab1 AS (SELECT peer, "Date", state
                      FROM timetracking
                      WHERE state = 2
                      GROUP BY peer, state, "Date"
                      ORDER BY "Date")
        SELECT peer
        FROM (SELECT *
                 FROM tab1
                 WHERE "Date" >= ((SELECT "Date" FROM tab1 ORDER BY 1 DESC LIMIT 1) - (N_DAYS || 'day')::INTERVAL)) b
        GROUP BY peer
        HAVING count(peer) > M_TIMES;
END;
$$ LANGUAGE plpgsql;

BEGIN;
    CALL not_less_times_last_days_exp_3_14(20,1);
    FETCH ALL rc;
COMMIT;

--Task 3/17
CREATE OR REPLACE PROCEDURE per_cent_early_entry(rc REFCURSOR = 'rc') AS $$
BEGIN
    OPEN rc FOR
        WITH t1 AS (SELECT EXTRACT(MONTH FROM "Date") AS Mon,
                           to_char("Date", 'Month')   AS Month,
                           count(peer)                AS TotalEntry
                    FROM timetracking
                             JOIN peers p ON p.nickname = timetracking.peer
                    WHERE state = 1
                      AND EXTRACT(MONTH FROM "Date") = EXTRACT(MONTH FROM birthday)
                    GROUP BY 1, 2),
             t2 AS (SELECT EXTRACT(MONTH FROM "Date") AS Mon,
                           to_char("Date", 'Month')   AS Month,
                           count(peer)                AS EarlyEntries
                    FROM timetracking
                             JOIN peers p ON p.nickname = timetracking.peer
                    WHERE state = 1
                      AND EXTRACT(MONTH FROM "Date") = EXTRACT(MONTH FROM birthday)
                      AND "Time" < '12:00:00'
                    GROUP BY 1, 2)
        SELECT t1.Month, round(EarlyEntries * 100 / TotalEntry::NUMERIC, 2) AS EarlyEntries
        FROM t1
                 JOIN t2 ON t1.Mon = t2.Mon;
END
$$ LANGUAGE plpgsql;

BEGIN;
    CALL per_cent_early_entry();
    FETCH ALL rc;
COMMIT;