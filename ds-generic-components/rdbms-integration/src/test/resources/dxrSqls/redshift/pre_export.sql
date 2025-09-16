CREATE OR REPLACE PROCEDURE create_test_table()
AS
$$
BEGIN
    EXECUTE 'DROP TABLE IF EXISTS [OWNERID].dxr_redshift_export_test';
    CREATE TABLE [OWNERID].dxr_redshift_export_test
    (
        db_schema   VARCHAR(255),
        sys_date    TIMESTAMP,
        db_name     VARCHAR(255),
        insert_date DATE DEFAULT current_date
    );
END
$$ LANGUAGE plpgsql;
CALL create_test_table();
