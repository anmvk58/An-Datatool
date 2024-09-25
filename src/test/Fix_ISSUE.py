from connector import connector_factory
from src.common.default_var import DefaultVar
if __name__ == '__main__':
    cnn = connector_factory.Connector_factory.create_connector("WH2_LIVE", config_path=DefaultVar.DEV_ENV)

    cursor = cnn.create_cursor()

    list = ['2023-05-14', '2023-05-15', '2023-05-16', '2023-05-17', '2023-05-18', '2023-05-19', '2023-05-20', '2023-05-21', '2023-05-22', '2023-05-23', '2023-05-24']
    for data_date in list:
        print(f"Run for {data_date}: ")
        data_date2 = data_date.replace("-", "")

        cursor.execute("truncate table tbl_vpb_tk_sodep_anmv_insert")
        cursor.execute("truncate table tbl_vpb_tk_sodep_anmv")
        print("Finish truncated! ")

        sql_make_insert = f"""
            INSERT INTO tbl_vpb_tk_sodep_anmv_insert
            SELECT 
                t.*
                --,CONCAT(t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE)
            FROM 
            (SELECT 
                SUBSTRING(a.VPB_TK_SODEP_CONCAT, CHARINDEX('-', a.VPB_TK_SODEP_CONCAT)+1, LEN(a.VPB_TK_SODEP_CONCAT)) VPB_TK_SODEP,
                a.STATUS,
                a.FEE,
                a.FEE_AMT,
                b.FEE_REASON,
                a.FEE_REF,
                a.GROUP_ACCT,
                b.INPUTTER,
                d.CUS_NAME SHORT_NAME,
                d.SEGMENT,
                a.CO_CODE,
                a.AUTHORISER,
                d.VIP_CODE VIS_TYPE,
                CAST('{data_date}' as DATE) EFF_DATE,
                CAST('2999-01-01' as DATE) EXP_DATE
            FROM 
                sodep1324 a 
                INNER JOIN sodepdetail1324 b ON a.VPB_TK_SODEP_CONCAT = b.VPB_TK_SODEP_CONCAT AND b.M = 1
                LEFT JOIN TBL_EFZ_ACCOUNT c ON a.GROUP_ACCT = c.ACCOUNT_NUMBER
                LEFT JOIN VPB_CUSTOMER d ON c.CUSTOMER = d.RECID
            WHERE  1 = 1  
                AND SUBSTRING(a.VPB_TK_SODEP_CONCAT, 0, 9) = '{data_date2}' AND
                NOT EXISTS (SELECT 1 FROM TBL_VPB_TK_SODEP_20230524 t WHERE SUBSTRING(a.VPB_TK_SODEP_CONCAT, CHARINDEX('-', a.VPB_TK_SODEP_CONCAT)+1, LEN(a.VPB_TK_SODEP_CONCAT)) = t.VPB_TK_SODEP)
            ) t
        """

        cursor.execute(sql_make_insert)
        print("Done step make insert")

        sql_make_update = f"""
            INSERT INTO tbl_vpb_tk_sodep_anmv
            SELECT 
                t.*,
                --CONCAT(t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE), 
                CONCAT(z.VPB_TK_SODEP,z.STATUS,z.FEE,z.FEE_AMT,z.FEE_REASON,z.FEE_REF,z.GROUP_ACCT,z.INPUTTER,z.SHORT_NAME,z.SEGMENT,z.CO_CODE,z.AUTHORISER,z.VIS_TYPE,z.EFF_DATE,z.EXP_DATE)
                --,CONCAT(t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE)
            FROM 
            (SELECT 
                SUBSTRING(a.VPB_TK_SODEP_CONCAT, CHARINDEX('-', a.VPB_TK_SODEP_CONCAT)+1, LEN(a.VPB_TK_SODEP_CONCAT)) VPB_TK_SODEP,
                a.STATUS,
                a.FEE,
                a.FEE_AMT,
                b.FEE_REASON,
                a.FEE_REF,
                a.GROUP_ACCT,
                b.INPUTTER,
                d.CUS_NAME SHORT_NAME,
                d.SEGMENT,
                a.CO_CODE,
                a.AUTHORISER,
                d.VIP_CODE VIS_TYPE,
                CAST('{data_date}' as DATE) EFF_DATE,
                CAST('2999-01-01' as DATE) EXP_DATE
            FROM 
                sodep1324 a 
                INNER JOIN sodepdetail1324 b ON a.VPB_TK_SODEP_CONCAT = b.VPB_TK_SODEP_CONCAT AND b.M = 1
                LEFT JOIN TBL_EFZ_ACCOUNT c ON a.GROUP_ACCT = c.ACCOUNT_NUMBER
                LEFT JOIN VPB_CUSTOMER d ON c.CUSTOMER = d.RECID
            WHERE  1 = 1  
                AND SUBSTRING(a.VPB_TK_SODEP_CONCAT, 0, 9) = '{data_date2}' AND
                EXISTS (SELECT 1 FROM TBL_VPB_TK_SODEP_20230524 t WHERE SUBSTRING(a.VPB_TK_SODEP_CONCAT, CHARINDEX('-', a.VPB_TK_SODEP_CONCAT)+1, LEN(a.VPB_TK_SODEP_CONCAT)) = t.VPB_TK_SODEP)
            ) t 
            INNER JOIN (
            SELECT t1.* FROM
            (SELECT *, ROW_NUMBER() OVER(PARTITION BY VPB_TK_SODEP ORDER BY EFF_DATE DESC) RowNumber FROM VPB_WHR2.dbo.TBL_VPB_TK_SODEP_20230524) t1 WHERE t1.RowNumber = 1
            ) z ON t.VPB_TK_SODEP = z.VPB_TK_SODEP  
            WHERE 
                CONCAT(t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE)
                <>
                CONCAT(z.VPB_TK_SODEP,z.STATUS,z.FEE,z.FEE_AMT,z.FEE_REASON,z.FEE_REF,z.GROUP_ACCT,z.INPUTTER,z.SHORT_NAME,z.SEGMENT,z.CO_CODE,z.AUTHORISER,z.VIS_TYPE,z.EFF_DATE,z.EXP_DATE);
        """

        cursor.execute(sql_make_update)
        print("Done step make update")

        sql_fix_1 = f"""
            UPDATE A
            SET EXP_DATE = '{data_date}'
            FROM TBL_VPB_TK_SODEP_20230524 A
            JOIN tbl_vpb_tk_sodep_anmv B
                ON A.VPB_TK_SODEP = B.VPB_TK_SODEP  AND CONCAT(A.VPB_TK_SODEP,A.STATUS,A.FEE,A.FEE_AMT,A.FEE_REASON,A.FEE_REF,A.GROUP_ACCT,A.INPUTTER,A.SHORT_NAME,A.SEGMENT,A.CO_CODE,A.AUTHORISER,A.VIS_TYPE,A.EFF_DATE,A.EXP_DATE) = B.Checkk
        """

        sql_fix_2 = """
            INSERT INTO TBL_VPB_TK_SODEP_20230524 SELECT t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE FROM tbl_vpb_tk_sodep_anmv t    
        """

        sql_fix_3 = """
            INSERT INTO TBL_VPB_TK_SODEP_20230524 SELECT t.VPB_TK_SODEP,t.STATUS,t.FEE,t.FEE_AMT,t.FEE_REASON,t.FEE_REF,t.GROUP_ACCT,t.INPUTTER,t.SHORT_NAME,t.SEGMENT,t.CO_CODE,t.AUTHORISER,t.VIS_TYPE,t.EFF_DATE,t.EXP_DATE FROM tbl_vpb_tk_sodep_anmv_insert t
        """

        cursor.execute(sql_fix_1)
        print("Fix 1 done !")
        cursor.execute(sql_fix_2)
        print("Fix 2 done !")
        cursor.execute(sql_fix_3)
        print("Fix 3 done !")
        print("------------------------------------------------")
