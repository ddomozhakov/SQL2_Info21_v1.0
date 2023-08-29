---- CREATE new database
CREATE DATABASE part4base;

CREATE TABLE TABLE_1
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLE_1 VALUES (1, 10);
INSERT INTO TABLE_1 VALUES (2, 20);
INSERT INTO TABLE_1 VALUES (3, 30);
INSERT INTO TABLE_1 VALUES (4, 40);
INSERT INTO TABLE_1 VALUES (5, 50);

CREATE TABLE TABLE_2
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLE_2 VALUES (1, 100);
INSERT INTO TABLE_2 VALUES (2, 200);
INSERT INTO TABLE_2 VALUES (3, 300);
INSERT INTO TABLE_2 VALUES (4, 400);
INSERT INTO TABLE_2 VALUES (5, 500);

CREATE TABLE TABLE_3
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLE_3 VALUES (1, 1000);
INSERT INTO TABLE_3 VALUES (2, 2000);
INSERT INTO TABLE_3 VALUES (3, 3000);
INSERT INTO TABLE_3 VALUES (4, 4000);
INSERT INTO TABLE_3 VALUES (5, 5000);

CREATE TABLE TABLEName_1
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLEName_1 VALUES (1, 10);
INSERT INTO TABLEName_1 VALUES (2, 20);
INSERT INTO TABLEName_1 VALUES (3, 30);
INSERT INTO TABLEName_1 VALUES (4, 40);
INSERT INTO TABLEName_1 VALUES (5, 50);

CREATE TABLE TABLEName_2
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLEName_2 VALUES (1, 100);
INSERT INTO TABLEName_2 VALUES (2, 200);
INSERT INTO TABLEName_2 VALUES (3, 300);
INSERT INTO TABLEName_2 VALUES (4, 400);
INSERT INTO TABLEName_2 VALUES (5, 500);

CREATE TABLE TABLEName_3
( col_1 bigint primary key ,
  col_2 bigint
  );
INSERT INTO TABLEName_3 VALUES (1, 1000);
INSERT INTO TABLEName_3 VALUES (2, 2000);
INSERT INTO TABLEName_3 VALUES (3, 3000);
INSERT INTO TABLEName_3 VALUES (4, 4000);
INSERT INTO TABLEName_3 VALUES (5, 5000);

CREATE OR REPLACE FUNCTION fnc_sql_1()
    RETURNS VOID AS
    $$
    ;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_sql_2(par_2_1 varchar, par_2_2 bigint)
    RETURNS VOID AS
    $$
    ;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_sql_3(par_3_1 varchar, par_3_2 bigint)
    RETURNS VOID AS
    $$
    ;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_sql_4(par_4_1 varchar, par_4_2 bigint)
    RETURNS VOID AS
    $$
    ;
    $$
    LANGUAGE SQL;

CREATE OR REPLACE FUNCTION fnc_print_INSERT_message() RETURNS TRIGGER AS $trg_INSERT_1$
    BEGIN
        RAISE NOTICE 'INSERT message!';
    END;
$trg_INSERT_1$ LANGUAGE plpgsql;

CREATE TRIGGER trg_INSERT_1
    AFTER INSERT
    ON TABLE_1
    FOR EACH ROW
EXECUTE PROCEDURE fnc_print_INSERT_message();

CREATE OR REPLACE FUNCTION fnc_print_update_message() RETURNS TRIGGER AS $trg_update_1$
    BEGIN
        RAISE NOTICE 'Update message!';
    END;
$trg_update_1$ LANGUAGE plpgsql;

CREATE TRIGGER trg_update_1
    AFTER UPDATE
    ON TABLE_2
    FOR EACH ROW
EXECUTE PROCEDURE fnc_print_update_message();

---- Task_1 delete TABLEName
CREATE OR REPLACE PROCEDURE TABLE_TABLE_delete()
LANGUAGE plpgsql AS
$BODY$
DECLARE
    row record;
BEGIN
    FOR row IN
        SELECT
            TABLE_schema,
            TABLE_name
        FROM
            information_schema.TABLEs
        WHERE
            TABLE_type = 'BASE TABLE' AND TABLE_schema = 'public'
        AND
            TABLE_name ILIKE ('TableName' || '%')
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.TABLE_schema) || '.' || quote_ident(row.TABLE_name);
    END LOOP;
END
$BODY$;

--Test delete TABLEName
CALL TABLE_TABLE_delete();
----

---- Task_2 func_list
CREATE OR REPLACE PROCEDURE sql_func_list( func_list OUT varchar, counter OUT bigint )
LANGUAGE plpgsql AS
$BODY$
DECLARE
    row record;
BEGIN
    func_list = '';
    counter = 0;
    FOR row IN
        SELECT 'Name: ' || pg_proc.proname || ' Parameters: ' ||
            pg_get_function_arguments(pg_proc.oid) as function
        FROM pg_proc
                LEFT JOIN pg_namespace ON pg_proc.pronamespace = pg_namespace.oid
        WHERE pg_namespace.nspname NOT IN ('pg_catalog', 'information_schema')
            AND pg_get_function_arguments(pg_proc.oid) NOT LIKE ''
        ORDER BY function
    LOOP
        func_list = func_list || row || E'\n';
        counter = counter + 1;
    END LOOP;
END
$BODY$;

--Test func_list
CALL sql_func_list('', 0);
----

---- Task_3 delete triggers
CREATE OR REPLACE PROCEDURE drop_all_triggers( counter OUT bigint )
LANGUAGE plpgsql AS
$BODY$
DECLARE
    triggername record;
BEGIN
    counter = 0;
    FOR triggername IN SELECT * FROM information_schema.triggers WHERE trigger_schema = 'public'
    LOOP
        EXECUTE 'DROP TRIGGER ' || triggername.trigger_name || ' ON ' || triggername.event_object_TABLE || ';';
        counter = counter + 1;
    END LOOP;
END
$BODY$;

--Test func_list
CALL drop_all_triggers(0);
SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = 'public';
----

---- Task_4 delete triggers
CREATE OR REPLACE PROCEDURE find_func_info(pattern IN varchar, rc refcursor = 'rc') AS $$
BEGIN
    OPEN rc FOR
        SELECT routine_name AS name, 'function' AS type
        FROM information_schema.routines
        WHERE routine_schema = 'public'
               AND routine_type = 'FUNCTION'
               AND (routine_name ~ pattern
               OR 'function' ~ pattern)
        UNION
        SELECT routine_name AS name, 'procedure' AS type
        FROM information_schema.routines
        WHERE routine_schema = 'public'
               AND routine_type = 'PROCEDURE'
               AND (routine_name ~ pattern
               OR 'procedure' ~ pattern)
        ORDER BY type;
END
$$ LANGUAGE plpgsql;

--Test func_list
BEGIN;
    CALL find_func_info('fnc');
    FETCH ALL rc;
COMMIT;
----
