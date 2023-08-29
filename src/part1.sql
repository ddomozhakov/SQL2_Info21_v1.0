drop table if exists Peers cascade;
drop table if exists Tasks cascade;
drop table if exists Checks cascade;
drop table if exists P2P cascade;
drop table if exists Verter cascade;
drop table if exists TransferredPoints cascade;
drop table if exists Friends cascade;
drop table if exists Recommendations cascade;
drop table if exists XP cascade;
drop table if exists TimeTracking cascade;
drop trigger if exists trg_p2p_transferred_points ON p2p;
drop function if exists fnc_trg_p2p_transferred_points();
drop trigger if exists trg_xp_check ON xp;
drop function if exists fnc_trg_xp_check();
drop sequence if exists seq_checks;
drop sequence if exists seq_p2p;
drop sequence if exists seq_verter;
drop sequence if exists seq_friends;
drop sequence if exists seq_recommendations;
drop sequence if exists seq_xp;
drop sequence if exists seq_timetracking;
drop sequence if exists seq_transferredpoints;
drop procedure if exists export_data(varchar);
drop procedure if exists import_data(varchar);
drop function if exists table_transferred_points();
drop function if exists peers_not_left_campus_in_date(varchar);
drop procedure if exists per_cent_of_peers_work_two_blocks(varchar, varchar, refcursor);
drop procedure if exists peer_points_changed_3_1(refcursor);
drop procedure if exists peers_finish_block_with_date(varchar, refcursor);
drop procedure if exists peers_finish_one_and_two_but_no_three(varchar, varchar, varchar, refcursor);
drop procedure if exists ahead_of_time(time, integer, refcursor);
drop procedure if exists per_cent_early_entry(refcursor);
drop procedure if exists lucky_days(integer, refcursor);
drop function if exists get_passed_3_2();
drop procedure if exists points_changed_3_4(refcursor);
drop procedure if exists tasks_frequency_3_6(refcursor);
drop procedure if exists tasks_recommended_3_8(refcursor);
drop procedure if exists state_percent_3_10(refcursor);
drop procedure if exists tasks_count_3_12(refcursor);
drop procedure if exists max_exp_3_14(refcursor);
drop procedure if exists not_less_times_last_days_exp_3_14(bigint, bigint, refcursor);
drop procedure if exists add_p2p_check(checked_peer varchar, checking_peer varchar, task_title varchar, "state" check_status, check_time time);
drop procedure if exists add_verter_check(checked_peer varchar, task_title varchar, "state" check_status, check_time time);
drop type if exists check_status;
drop database if exists school21;

create database school21;

create table Peers
( Nickname varchar primary key,
  Birthday date
  );

create table Tasks
( Title varchar primary key,
  ParentTask varchar,
  MaxXP bigint not null,
  constraint fk_tasks_parent_task foreign key (ParentTask) references Tasks(Title)
  );

create table Checks
( ID bigint primary key,
  Peer varchar,
  Task varchar,
  "Date" date,
  constraint fk_checks_peers_nickname foreign key (Peer) references Peers(NickName),
  constraint fk_checks_tasks_title foreign key (Task) references Tasks(Title)
  );
CREATE SEQUENCE IF NOT EXISTS seq_checks AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE Checks ALTER COLUMN ID SET DEFAULT nextval('seq_checks');

create type check_status as enum ('Start', 'Success', 'Failure');

create table P2P
( ID bigint primary key,
  "Check" bigint,
  CheckingPeer varchar,
  State check_status NOT NULL,
  "Time" time,
  constraint fk_p2p_checks_id foreign key ("Check") references Checks(ID),
  constraint fk_p2p_peers_nickname foreign key (CheckingPeer) references Peers(NickName)
  );
ALTER TABLE P2P ADD CONSTRAINT ch_p2p_status CHECK (State IN ('Start', 'Success', 'Failure'));
CREATE SEQUENCE IF NOT EXISTS seq_p2p AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE P2P ALTER COLUMN ID SET DEFAULT nextval('seq_p2p');

create table Verter
( ID bigint primary key,
  "Check" bigint,
  State check_status,
  "Time" time,
  constraint fk_verter_checks_id foreign key ("Check") references Checks(ID)
  );
ALTER TABLE Verter ADD CONSTRAINT ch_verter_status CHECK (State IN ('Start', 'Success', 'Failure'));
CREATE SEQUENCE IF NOT EXISTS seq_verter AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE Verter ALTER COLUMN ID SET DEFAULT nextval('seq_verter');

create table TransferredPoints
( ID bigint primary key,
  CheckingPeer varchar,
  CheckedPeer varchar,
  PointsAmount bigint,
  constraint fk_transferred_points_peers_nickname_checking foreign key (CheckingPeer) references Peers(NickName),
  constraint fk_transferred_points_peers_nickname_checked foreign key (CheckedPeer) references Peers(NickName)
  );
ALTER TABLE TransferredPoints ADD CONSTRAINT ch_not_same_peer CHECK (CheckingPeer NOT LIKE CheckedPeer);
CREATE SEQUENCE IF NOT EXISTS seq_transferredpoints AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE TransferredPoints ALTER COLUMN ID SET DEFAULT nextval('seq_transferredpoints');
CREATE UNIQUE INDEX IF NOT EXISTS idx_transferred_points_unique ON TransferredPoints(CheckingPeer, CheckedPeer);

create table Friends
( ID bigint primary key,
  Peer1 varchar,
  Peer2 varchar,
  constraint fk_friends_peers_nickname_peer_1 foreign key (Peer1) references Peers(NickName),
  constraint fk_friends_peers_nickname_peer_2 foreign key (Peer2) references Peers(NickName)
  );
ALTER TABLE Friends ADD CONSTRAINT ch_friends_not_same_peer CHECK (Peer1 NOT LIKE Peer2);
CREATE SEQUENCE IF NOT EXISTS seq_friends AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE Friends ALTER COLUMN ID SET DEFAULT nextval('seq_friends');

create table Recommendations
( ID bigint primary key,
  Peer varchar,
  RecommendedPeer varchar,
  constraint fk_recommendations_peers_nickname_peer foreign key (Peer) references Peers(NickName),
  constraint fk_recommendations_peers_nickname_recommendedPeer foreign key (RecommendedPeer) references Peers(NickName)
  );
ALTER TABLE Recommendations ADD CONSTRAINT ch_recommended_not_same_peer CHECK (Peer NOT LIKE RecommendedPeer);
CREATE SEQUENCE IF NOT EXISTS seq_recommendations AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE Recommendations ALTER COLUMN ID SET DEFAULT nextval('seq_recommendations');

create table XP
( ID bigint primary key,
  "Check" bigint,
  XPAmount bigint,
  constraint fk_xp_checks_id foreign key ("Check") references Checks(ID)
  );
CREATE SEQUENCE IF NOT EXISTS seq_xp AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE XP ALTER COLUMN ID SET DEFAULT nextval('seq_xp');

create table TimeTracking
( ID bigint primary key,
  Peer varchar,
  "Date" date,
  "Time" time,
  State int,
  constraint fk_timetracking_nickname_peer foreign key (Peer) references Peers(NickName)
  );
ALTER TABLE TimeTracking ADD CONSTRAINT ch_timetracking_status CHECK (State IN (1, 2));
CREATE SEQUENCE IF NOT EXISTS seq_timetracking AS BIGINT START WITH 1 INCREMENT BY 1;
ALTER TABLE TimeTracking ALTER COLUMN ID SET DEFAULT nextval('seq_timetracking');

CREATE OR REPLACE PROCEDURE import_data(separator varchar) AS $$
DECLARE
--     curr_dir varchar := 'c:/' || getpgusername() || '/import_';    -- For Windows
    curr_dir varchar := '/Users/'||getpgusername()||'/SQL2_Info21_v1.0-1/src/import_';
    t_name varchar;
    t_array varchar[] := ARRAY['peers', 'tasks', 'checks', 'friends', 'p2p',
        'recommendations', 'timetracking', 'transferredpoints', 'verter', 'xp'];
BEGIN
    FOREACH t_name IN ARRAY t_array
    LOOP
        EXECUTE format('COPY %I FROM %L DELIMITER %L CSV HEADER;', t_name, curr_dir || t_name || '.csv', $1);
		IF t_name NOT IN ('peers', 'tasks') THEN
            EXECUTE format('SELECT setval(%L, (SELECT coalesce(MAX(id), 0) + 1 FROM %I), FALSE);', 'seq_' || t_name, t_name);
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE export_data(separator varchar) AS $$
DECLARE
--     curr_dir varchar := 'c:/' || getpgusername() || '/export_';    -- For Windows
    curr_dir varchar := '/Users/'||getpgusername()||'/SQL2_Info21_v1.0-1/src/export_';
    t_name varchar;
BEGIN
    FOR t_name IN SELECT table_name FROM information_schema.tables WHERE table_schema LIKE 'public'
    LOOP
        EXECUTE format(
            'COPY (SELECT * FROM %I) TO %L DELIMITER %L CSV HEADER;', t_name, curr_dir || t_name || '.csv', $1);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

TRUNCATE Checks, Friends, P2P, Peers, Recommendations, Tasks, TimeTracking, TransferredPoints, Verter, XP;  -- Call before import
CALL import_data(',');
CALL export_data(',');