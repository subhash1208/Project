BEGIN;
    ALTER TABLE [OWNERID].dxr_redshift_export_test
        ADD COLUMN test_col DATE DEFAULT CURRENT_DATE;
    ANALYZE [OWNERID].dxr_redshift_export_test;
END;
