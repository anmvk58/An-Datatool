import pandas as pd
from src.connector.connector_factory import Connector_factory
from src.common.default_var import DefaultVar
from src.compare_module.Compare_object import Compare_Object_Query

if __name__ == '__main__':
    query1 = """
        WITH 
        FT AS 
            (SELECT tft.REF_NO,tft.DEBIT_ACCT_NO,SENDING_ACCT,R_CI_CODE,BC_BANK_SORT_CODE,VPB_SERVICE,SERVICE_CHANNEL,TRANSACTION_TYPE,PR_CARD_NO,AC.ACCOUNT_TITLE_1
                ,LISTAGG(ORDERING_BANK) within group (order by tftd.M,tftd.S) AS ORDERING_BANK
                ,LISTAGG(ORDERING_CUST) WITHIN GROUP (order by tftd.M,tftd.S) AS ORDERING_CUST
                ,LISTAGG(IN_ORDERING_BK) WITHIN GROUP (order by tftd.M,tftd.S) AS IN_ORDERING_BK
                ,LISTAGG(TXN_DETAIL_VPB)  WITHIN GROUP (order by tftd.M,tftd.S) AS TXN_DETAIL_VPB
                ,LISTAGG(PAYMENT_DETAILS) WITHIN GROUP (order by tftd.M,tftd.S) AS PAYMENT_DETAILS
            FROM T24_FUNDS_TRANSFER tft 
                LEFT JOIN T24_FUNDS_TRANSFER_DETAILS tftd ON tft.REF_NO = tftd.REF_NO
                LEFT JOIN T24_ACCOUNT_DETAILS AC ON AC.ACCOUNT_NUMBER = TFT.DEBIT_ACCT_NO AND AC.M = 1 AND AC.S =1
            GROUP BY tft.REF_NO,tft.DEBIT_ACCT_NO,SENDING_ACCT,R_CI_CODE,BC_BANK_SORT_CODE,VPB_SERVICE,SERVICE_CHANNEL,TRANSACTION_TYPE,PR_CARD_NO,AC.ACCOUNT_TITLE_1),
        FTH AS (
            SELECT fth.REF_NO,DEBIT_ACCT_NO, SENDING_ACCT,R_CI_CODE,BC_BANK_SORT_CODE,VPB_SERVICE,SERVICE_CHANNEL,TRANSACTION_TYPE,PR_CARD_NO,ACC.ACCOUNT_TITLE_1
                ,LISTAGG(ORDERING_BANK) WITHIN GROUP (ORDER BY fthd.M,fthd.S) AS ORDERING_BANK
                ,LISTAGG(ORDERING_CUST) WITHIN GROUP (ORDER BY fthd.M,fthd.S) AS ORDERING_CUST
                ,LISTAGG(IN_ORDERING_BK) WITHIN GROUP (ORDER BY fthd.M,fthd.S) AS IN_ORDERING_BK
                ,LISTAGG(TXN_DETAIL_VPB)  WITHIN GROUP (ORDER BY fthd.M,fthd.S) AS TXN_DETAIL_VPB
                ,LISTAGG(PAYMENT_DETAILS) WITHIN GROUP (ORDER BY fthd.M,fthd.S) AS PAYMENT_DETAILS
            FROM T24_FUNDS_TRANSFER_HIS fth 
                LEFT JOIN T24_FUNDS_TRANSFER_HIS_DETAILS fthd ON fth.REF_NO = fthd.REF_NO
                LEFT JOIN T24_ACCOUNT_DETAILS ACC ON ACC.ACCOUNT_NUMBER = FTH.DEBIT_ACCT_NO AND ACC.M = 1 AND ACC.S =1
            GROUP BY fth.REF_NO,DEBIT_ACCT_NO,SENDING_ACCT,R_CI_CODE,BC_BANK_SORT_CODE,VPB_SERVICE,SERVICE_CHANNEL,TRANSACTION_TYPE,PR_CARD_NO,ACC.ACCOUNT_TITLE_1)
        SELECT v.RECID,
        v.ONLINE_ACTUAL_BAL,
        v.VALUE_DATE,
        v.BOOKING_DATE,
        v.TRANS_REFERENCE,
        nvl(narr.NARRATIVE, v.NARRATIVE) AS NARRATIVE,
        v.TRANSACTION_CODE,
        v.ACCOUNT_NUMBER,
        v.AMOUNT_LCY,
        v.AMOUNT_FCY,  
        v.CURRENCY,
        to_char(v.update_time,'yymmddhh24mi') AS date_time,
        to_char(v.update_time,'yymmddhh24mi') AS update_time,
        nvl(ft.SENDING_ACCT,h.SENDING_ACCT) AS SENDING_ACCT,
        nvl(ft.ORDERING_BANK,h.ORDERING_BANK) AS ORDERING_BANK,
        nvl(ft.ORDERING_CUST,h.ORDERING_CUST) AS ORDERING_CUST,
        nvl(ft.IN_ORDERING_BK,h.IN_ORDERING_BK) AS IN_ORDERING_BK
        ,CASE WHEN nvl(ft.TXN_DETAIL_VPB,h.TXN_DETAIL_VPB) IS NULL THEN nvl(ft.PAYMENT_DETAILS,h.PAYMENT_DETAILS)
            ELSE nvl(ft.TXN_DETAIL_VPB,h.TXN_DETAIL_VPB) END AS PAYMENT_DETAILS
        ,nvl(ft.R_CI_CODE,h.R_CI_CODE) AS R_CI_CODE
        ,NVL(FT.BC_BANK_SORT_CODE,H.BC_BANK_SORT_CODE) AS BC_BANK_SORT_CODE
        ,NVL(FT.VPB_SERVICE,H.VPB_SERVICE) AS VPB_SERVICE
        ,NVL(FT.SERVICE_CHANNEL,H.SERVICE_CHANNEL) AS SERVICE_CHANNEL
        ,NVL(FT.TRANSACTION_TYPE,H.TRANSACTION_TYPE) AS TRANSACTION_TYPE 
        ,NVL(FT.PR_CARD_NO,H.PR_CARD_NO) AS PR_CARD_NO
        ,NVL(FT.ACCOUNT_TITLE_1,H.ACCOUNT_TITLE_1) AS DEBIT_ACCOUNT_NAME
        ,nvl(FT.DEBIT_ACCT_NO,H.DEBIT_ACCT_NO) AS DEBIT_ACCT_NO
        ,v.record_status
        FROM T24_VPB_STMT_ENTRY v
        LEFT JOIN (SELECT * FROM T24_VPB_NARRATIVE_HIST UNION ALL SELECT * FROM T24_VPB_NARRATIVE) narr ON v.RECID = narr.RECID
        LEFT JOIN FT ON  v.OUR_REFERENCE = ft.REF_NO
        LEFT JOIN (SELECT * FROM FTH WHERE FTH.REF_NO NOT IN (SELECT ft.REF_NO ||';1' FROM FT)) H ON  v.OUR_REFERENCE || ';1' = H.REF_NO
        WHERE
        v.ACCOUNT_NUMBER in  ('263528093')
        AND v.BOOKING_DATE >= '20211013'
        AND v.BOOKING_DATE  <= '20211013'
        AND (v.AMOUNT_LCY<> 0 OR v.AMOUNT_FCY <>0)
        AND v.RECID NOT LIKE 'F%'
    """
    # coq1 = Compare_Object_Query(src_name='RDS_DIH', query=query1)
    # meta_data, total = coq1.cnn.get_meta_data_from_query(query1)
    # print(meta_data)
    # df1 = coq1.get_data_after_process()

    cnn = Connector_factory.create_connector('RDS_DIH', DefaultVar.DEV_ENV)
    cursor = cnn.create_cursor()
    cursor.execute(query1)
    df = cnn.read_sql_query(query1)
    print(df)
    print("Hello")


