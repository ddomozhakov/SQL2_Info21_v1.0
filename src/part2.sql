---- Add new p2p check
CREATE OR REPLACE PROCEDURE add_p2p_check(checked_peer varchar, checking_peer varchar, task_title varchar,
                                          "state" check_status, check_time time)
    LANGUAGE plpgsql AS
$BODY$
BEGIN
    IF "state" = 'Start' THEN
        INSERT INTO checks(id, peer, task, "Date")
        VALUES (default, checked_peer, task_title, (SELECT CURRENT_DATE));
        INSERT INTO p2p(id, "Check", checkingpeer, "state", "Time")
        VALUES (default, (SELECT COALESCE(MAX(id), 0) FROM checks), checking_peer, "state", check_time);
    ELSE
        IF (SELECT COUNT(*)
            FROM p2p
            WHERE p2p."Check" = (SELECT COALESCE(MAX(id), 0)
                                 FROM checks
                                 WHERE checks.task = task_title
                                   AND checks.peer = checked_peer)
              AND p2p.checkingpeer = checking_peer
              AND p2p."state" = 'Start') = 0
        THEN
            RAISE NOTICE 'START TASK IS REQUIRED';
        ELSE
            INSERT INTO p2p(id, "Check", checkingpeer, "state", "Time")
            VALUES (default, (SELECT COALESCE(MAX(id), 0)
                              FROM checks
                              WHERE checks.task = task_title
                                AND checks.peer = checked_peer), checking_peer, "state", check_time);
        END IF;
    END IF;
END
$BODY$;

----  Test add_p2p_check
CALL add_p2p_check('BerryPeer', 'austpowe',
    'C7_S21_SmartCalc_v1.0', 'Success', '14:10:00');
CALL add_p2p_check('BerryPeer', 'austpowe',
    'C7_S21_SmartCalc_v1.0', 'Start', '13:40:00');
CALL add_p2p_check('BerryPeer', 'austpowe',
    'C7_S21_SmartCalc_v1.0', 'Success', '14:10:00');
----

---- Add new verter check
CREATE OR REPLACE PROCEDURE add_verter_check(checked_peer varchar, task_title varchar,
                                             "state" check_status, check_time time)
    LANGUAGE plpgsql AS
$BODY$
BEGIN
    IF "state" = 'Start' THEN
        INSERT INTO verter(id, "Check", "state", "Time")
        VALUES (default,
                (SELECT COALESCE(MAX(checks.id), 0)
                 FROM checks
                          JOIN p2p on checks.id = p2p."Check"
                 WHERE checks.task = task_title
                   AND checks.peer = checked_peer
                   AND p2p."state" = 'Success'),
                "state", check_time);
    ELSE
        IF (SELECT COUNT(*)
            FROM verter
            WHERE verter."Check" = (SELECT COALESCE(MAX(id), 0)
                                    FROM checks
                                    WHERE checks.task = task_title
                                      AND checks.peer = checked_peer)
              AND verter."state" = 'Start') = 0
        THEN
            RAISE NOTICE 'START TASK IS REQUIRED';
        ELSE
            INSERT INTO verter(id, "Check", "state", "Time")
            VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM verter),
                    (SELECT COALESCE(MAX(checks.id), 0)
                     FROM checks
                              JOIN p2p p on checks.id = p."Check"
                     WHERE checks.task = task_title
                       AND checks.peer = checked_peer
                       AND p."state" = 'Success'),
                    "state", check_time);
        END IF;
    END IF;
END
$BODY$;

---- Test add_verter_check
CALL add_verter_check('BerryPeer',
    'C7_S21_SmartCalc_v1.0', 'Success', '14:12:00');
CALL add_verter_check('BerryPeer',
    'C7_S21_SmartCalc_v1.0', 'Start', '14:10:00');
CALL add_verter_check('BerryPeer',
    'C7_S21_SmartCalc_v1.0', 'Success', '14:12:00');
----

---- Add new trigger for TransferredPoints
CREATE OR REPLACE FUNCTION fnc_trg_p2p_transferred_points() RETURNS TRIGGER AS
$trg_p2p_transferred_points$
DECLARE
    p2p_checkingpeer VARCHAR := (SELECT checkingpeer
                                 FROM p2p
                                 ORDER BY id DESC
                                 LIMIT 1);
    p2p_checkedpeer  VARCHAR := (SELECT peer
                                 FROM checks
                                 WHERE id = (SELECT "Check" FROM p2p ORDER BY id DESC LIMIT 1));
BEGIN
    IF (SELECT p2p.state FROM p2p ORDER BY p2p.id DESC LIMIT 1) = 'Start' THEN
        IF (SELECT id
            FROM transferredpoints
            WHERE checkingpeer = p2p_checkingpeer
              AND checkedpeer = p2p_checkedpeer) IS NOT NULL THEN
            UPDATE transferredpoints
            SET pointsamount = pointsamount + 1
            WHERE checkingpeer = p2p_checkingpeer
              AND checkedpeer = p2p_checkedpeer;
        ELSE
            INSERT INTO transferredpoints(id, checkingpeer, checkedpeer, pointsamount)
            VALUES (default,
                    (SELECT p2p.checkingpeer FROM p2p ORDER BY p2p.id DESC LIMIT 1),
                    (SELECT checks.peer
                     FROM checks
                              JOIN p2p ON checks.id = p2p."Check"
                     ORDER BY p2p.id DESC
                     LIMIT 1),
                    1);
        END IF;
    END IF;
    RETURN NULL;
END
$trg_p2p_transferred_points$ LANGUAGE plpgsql;


CREATE TRIGGER trg_p2p_transferred_points
    AFTER INSERT
    ON p2p
    FOR EACH ROW
EXECUTE FUNCTION fnc_trg_p2p_transferred_points();


---- Test p2p_transferred_points
CALL add_p2p_check('BerryPeer', 'austpowe',
    'CPP1_s21_matrix+', 'Start', '17:00:00');
CALL add_p2p_check('BerryPeer', 'austpowe',
    'CPP1_s21_matrix+', 'Success', '17:45:00');
CALL add_p2p_check('BerryPeer', 'TommyGun',
    'CPP1_s21_matrix+', 'Start', '18:45:00');
CALL add_p2p_check('BerryPeer', 'TommyGun',
    'CPP1_s21_matrix+', 'Success', '18:45:00');
----

---- Add new trigger for xp
CREATE OR REPLACE FUNCTION fnc_trg_xp_check() RETURNS TRIGGER AS
$trg_xp_check$
BEGIN
    IF (
                NEW.xpamount <=
                (SELECT tasks.maxxp
                 FROM tasks
                          JOIN checks on tasks.title = checks.task
                 WHERE NEW."Check" = checks.id)
            AND
                NEW."Check" IN
                (SELECT checks.id
                 FROM checks
                          LEFT JOIN p2p ON checks.id = p2p."Check"
                          LEFT JOIN verter ON checks.id = verter."Check"
                 WHERE NEW."Check" = checks.id
                   AND p2p.state = 'Success'
                   AND (verter.state = 'Success' OR verter.state IS NULL))
        )
    THEN
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$trg_xp_check$ LANGUAGE plpgsql;

CREATE TRIGGER trg_xp_check
    BEFORE INSERT ON xp
    FOR
    EACH ROW EXECUTE FUNCTION fnc_trg_xp_check();

---- Test trigger for xp
INSERT INTO xp(id, "Check", xpamount) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 313, 250);
INSERT INTO xp(id, "Check", xpamount) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 21, 500);
INSERT INTO xp(id, "Check", xpamount) VALUES ((SELECT COALESCE(MAX(id), 0) + 1 FROM xp), 21, 250);
----

--- Delete test data from tables
DELETE FROM p2p WHERE id BETWEEN 39 AND 44;
DELETE FROM verter WHERE id = 33 OR id = 34;
DELETE FROM xp WHERE id = 16;
DELETE FROM checks WHERE id BETWEEN 20 AND 22;
DELETE FROM transferredpoints WHERE id = 14;
UPDATE transferredpoints SET pointsamount = 3 WHERE id = 3;
DELETE FROM checks WHERE id BETWEEN 20 AND 22;
---
