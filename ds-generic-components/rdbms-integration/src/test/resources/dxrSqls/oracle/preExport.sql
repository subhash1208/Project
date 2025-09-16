BEGIN
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE [OWNERID].DXR_EXPORT_TEST';
    EXCEPTION
        WHEN OTHERS THEN
            IF
                sqlcode !=-942
            THEN
                RAISE;
            END IF;
    END;
    BEGIN
        EXECUTE IMMEDIATE 'DROP TABLE [OWNERID].DXR_EXPORT_TEST_STG';
    EXCEPTION
        WHEN OTHERS THEN
            IF
                sqlcode !=-942
            THEN
                RAISE;
            END IF;
    END;
    BEGIN
        EXECUTE IMMEDIATE q'[CREATE TABLE [OWNERID].DXR_EXPORT_TEST 
                        ( 
                            db_schema VARCHAR2(255 byte), 
                            sys_date   TIMESTAMP(6), 
                            db_name   VARCHAR2(255 byte), 
                            insert_date DATE DEFAULT SYSDATE NOT NULL 
                        )]'
;
    END;
END;