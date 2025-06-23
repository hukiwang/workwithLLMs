drop PROCEDURE if exists SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE;
create or replace PROCEDURE SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE(IN P_INPUT_JOB_SESSION VARCHAR(255),IN P_INPUT_DATADATE VARCHAR(8),INOUT P_OUT_WF_STATUS VARCHAR(1))
 AS


/********************************************************************************
# VERSION INFORMATION        : V1.0
# PROCEDURE DESCRIPTION     : 手续费调用表
# TARGET TABLE              : SRC.LI_SRC_ACC_H_T_COMMISSION_FEE
# SOURCE TABLE              : SRC.LI_SRC_ACC_T_COMMISSION_FEE

# CREATE DATE               : 2022-02-11
# CREATE BY                 : yto
# MODIFIED HISTORY          : 
# MODIFY_NAME    MODIFY_DATE        MODIFY_DETAIL
# 
********************************************************************************/

DECLARE V_WF_STATUS VARCHAR(1) DEFAULT 'S';
DECLARE V_WF_NAME VARCHAR(255) DEFAULT 'WF_LI_SRC_ACC_H_T_COMMISSION_FEE';
DECLARE V_BELONG_LEVEL VARCHAR(3) DEFAULT 'SRC';
BEGIN


/**************主体功能代码*********BEGIN*********************************/
--拉链回退:删除跑批日之后的数据
DELETE
FROM SRC.LI_SRC_ACC_H_T_COMMISSION_FEE
WHERE TO_CHAR(START_DATE, 'YYYYMMDD') >= :P_INPUT_DATADATE;

--将结束日期结束日期为跑批日的设置为MAX
UPDATE SRC.LI_SRC_ACC_H_T_COMMISSION_FEE
SET END_DATE = TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')
WHERE TO_CHAR(END_DATE, 'YYYYMMDD') >= :P_INPUT_DATADATE AND TO_CHAR(END_DATE,'YYYYMMDD') < '99991231';

DROP TABLE IF EXISTS SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE_MID_TMP;
CREATE TABLE SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE_MID_TMP WITH(orientation=column) AS 
SELECT
    T.FEE_ID               --费用id
    ,T.PAY_MODE            --付款方式代码
    ,T.CASH_ACCOUNT        --保险公司收付费银行帐号
    ,T.CASH_BANK           --保险公司收付费银行代码
    ,T.PRODUCT_ID          --产品ID
    ,T.CHARGE_TYPE         --缴费类型
    ,T.POLICY_YEAR         --险种缴费年度
    ,T.RECONCILIATION_CODE --对账单码
    ,T.PAY_CHANNEL         --支付渠道
    ,T.POLICY_TYPE         --保单类型
    ,T.CHEQUE_CODE         --支票号码
    ,T.SEND_ID             --银行转账底单序号
    ,T.ENTRY_CODE          --银行进账单号
    ,T.SOURCE_FEE_ID       --费用来源ID
    ,T.FINISH_TIME         --记账日期
    ,T.FEE_TYPE            --费用类型
    ,T.FEE_NAME            --费用名称
    ,T.ORGAN_ID            --人员直属机构id
    ,T.FINANCIAL_ORGAN     --人员直属机构最近一级独立核算机构
    ,T.CASHER_ORGAN        --出纳所属机构最近一级独立核算机构
    ,T.MANAGE_ORGAN_ID     --管理机构id
    ,T.MANAGEMENT_FINANCIAL_ORGAN --管理机构最近一级独立核算机构
    ,T.AGENCY_CODE         --中介机构代码
    ,T.AGENCY_BANK_CODE    --代理银行代码
    ,T.AGENT_CATE          --代理人销售渠道
    ,T.FEE_STATUS          --费用状态
    ,T.FEE_AMOUNT          --费用金额
    ,T.POLICY_ID           --保单id
    ,T.HEAD_ID             --人员所属总公司id
    ,T.DEPT_ID             --人员直属部门代码
    ,T.AGENT_ID            --业务员id
    ,T.PRODUCT_NUM         --投保单位险种序号
    ,T.POLICY_PERIOD       --险种实际缴费期次
	,T.MONEY_ID          --币种id
    ,T.POSTED           --是否已经生成记账凭证
    ,T.COMM_FEE_TYPE    --费用类别
    ,T.CRED_ID          --记账费用id
    ,T.RELATED_ID       --相关费用id
    ,T.NORMAL_PREMI     --原始保费金额
    ,T.COMMISION_RATE   --手续费率
    ,T.IS_CORRECT       --实付纠错
    ,T.AGENT_CODE       --业务员工号
    ,T.SEQUENCE_ID      --人管系统流水号
    ,T.BATCH_NO         --业务批次号
    ,T.AUDIT_STATUS     --支付审核标识
    ,T.BRANCH_ID        --人员所属分公司ID
    ,T.PERIOD_TYPE      --产品期限
    ,T.GL_PRODUCT_TYPE  --产品财务大类
    ,T.DEBETI           --产品混合合同的分拆及重大保险风险测试
    ,T.BENEFIT_TYPE     --产品责任分类
    ,T.BUSI_TYPE        --险种大类代码
    ,T.INTERNAL_ID      --产品代码
    ,T.AGENCY_TYPE      --中介机构类型
    ,T.AGENT_SUB_CATE   --代理人子分类
    ,T.LABOR_FORM       --代理人用工属性
    ,T.CASH_BANK_NAME   --保险公司收付费银行名称
    ,T.SYSTEM_SOURCE    --系统来源
    ,T.TRANS_ID         --交易ID
    ,T.STATUS           --记账状态
    ,T.POLICY_AGENCY_TYPE   --保单产险/健康险销售人员的中介机构类型
    ,T.POLICY_AGENT_CATE    --保单产险/健康险销售人员的类别
    ,T.POLICY_AGENT_CODE    --保单产险/健康险销售人员
    ,T.IS_CLEAR         --是否清账
    ,T.INSERT_TIME      --记录生成日期
    ,T.UPDATE_TIME      --记录更新日期
    ,T.OPER_FLAG        --操作状态
FROM SRC.LI_SRC_ACC_T_COMMISSION_FEE AS T
WHERE TO_CHAR(T.DP_TIME, 'YYYYMMDD') = :P_INPUT_DATADATE
;

/**************更新原拉链 BEGIN**************/
UPDATE SRC.LI_SRC_ACC_H_T_COMMISSION_FEE SOURCE_T
SET SOURCE_T.END_DATE = TO_DATE(:P_INPUT_DATADATE)
FROM SRC.LI_SRC_ACC_H_T_COMMISSION_FEE SOURCE_T, SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE_MID_TMP AS INCRE_T
WHERE SOURCE_T.FEE_ID = INCRE_T.FEE_ID
  AND SOURCE_T.END_DATE = TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS');
/**************更新原拉链 END**************/


/**************插入新拉链 BEGIN**************/
INSERT INTO SRC.LI_SRC_ACC_H_T_COMMISSION_FEE(
    FEE_ID
    ,PAY_MODE
    ,CASH_ACCOUNT
    ,CASH_BANK
    ,PRODUCT_ID
    ,CHARGE_TYPE
    ,POLICY_YEAR
    ,RECONCILIATION_CODE
    ,PAY_CHANNEL
    ,POLICY_TYPE
    ,CHEQUE_CODE
    ,SEND_ID
    ,ENTRY_CODE
    ,SOURCE_FEE_ID
    ,FINISH_TIME
    ,FEE_TYPE
    ,FEE_NAME
    ,ORGAN_ID
    ,FINANCIAL_ORGAN
    ,CASHER_ORGAN
    ,MANAGE_ORGAN_ID
    ,MANAGEMENT_FINANCIAL_ORGAN
    ,AGENCY_CODE
    ,AGENCY_BANK_CODE
    ,AGENT_CATE
    ,FEE_STATUS
	
	,FEE_AMOUNT
    ,POLICY_ID
    ,HEAD_ID
    ,DEPT_ID
    ,AGENT_ID
    ,PRODUCT_NUM
    ,POLICY_PERIOD
    ,MONEY_ID
    ,POSTED
    ,COMM_FEE_TYPE
    ,CRED_ID
    ,RELATED_ID
    ,NORMAL_PREM
    ,COMMISSION_RATE
    ,IS_CORRECT
    ,AGENT_CODE
    ,SEQUENCE_ID
    ,BATCH_NO
    ,AUDIT_STATUS
    ,BRANCH_ID
    ,PERIOD_TYPE
    ,GL_PRODUCT_TYPE
    ,DEBT_TYPE
    ,BENEFIT_TYPE
    ,BUSI_TYPE
    ,INTERNAL_ID
    ,AGENCY_TYPE
    ,AGENT_SUB_CATE
    ,LABOR_FORM
    ,CASH_BANK_NAME
    ,SYSTEM_SOURCE
    ,TRANS_ID
    ,STATUS
    ,POLICY_AGENCY_TYPE
    ,POLICY_AGENT_CATE
    ,POLICY_AGENT_CODE
    ,IS_CLEAR
    ,INSERT_TIME
    ,UPDATE_TIME
    ,STATUS_FLAG
    ,START_DATE
    ,END_DATE
    ,DP_TIME
)
SELECT
    T.FEE_ID               --费用id
    ,T.PAY_MODE            --付款方式代码
    ,T.CASH_ACCOUNT        --保险公司收付费银行帐号
    ,T.CASH_BANK           --保险公司收付费银行代码
    ,T.PRODUCT_ID          --产品ID
    ,T.CHARGE_TYPE         --缴费类型
    ,T.POLICY_YEAR         --险种缴费年度
    ,T.RECONCILIATION_CODE --对账单码
    ,T.PAY_CHANNEL         --支付渠道
    ,T.POLICY_TYPE         --保单类型
    ,T.CHEQUE_CODE         --支票号码
    ,T.SEND_ID             --银行转账底单序号
    ,T.ENTRY_CODE          --银行进账单号
    ,T.SOURCE_FEE_ID       --费用来源ID
    ,T.FINISH_TIME         --记账日期
    ,T.FEE_TYPE            --费用类型
    ,T.FEE_NAME            --费用名称
    ,T.ORGAN_ID            --人员直属机构id
    ,T.FINANCIAL_ORGAN     --人员直属机构最近一级独立核算机构
    ,T.CASHER_ORGAN        --出纳所属机构最近一级独立核算机构
    ,T.MANAGE_ORGAN_ID     --管理机构id
    ,T.MANAGEMENT_FINANCIAL_ORGAN --管理机构最近一级独立核算机构
    ,T.AGENCY_CODE         --中介机构代码
    ,T.AGENCY_BANK_CODE    --代理银行代码
    ,T.AGENT_CATE          --代理人销售渠道
    ,T.FEE_STATUS          --费用状态
    ,T.FEE_AMOUNT          --费用金额
    ,T.POLICY_ID           --保单id
    ,T.HEAD_ID             --人员所属总公司id
    ,T.DEPT_ID             --人员直属部门代码
    ,T.AGENT_ID            --业务员id
    ,T.PRODUCT_NUM         --投保单位险种序号
    ,T.POLICY_PERIOD       --险种实际缴费期次
    ,T.MONEY_ID            --币种id
    ,T.POSTED              --是否已经生成记账凭证
    ,T.COMM_FEE_TYPE       --费用类别
    ,T.CRED_ID             --记账凭证id
    ,T.RELATED_ID          --相关费用id
    ,T.NORMAL_PREM         --原始保费金额
    ,T.COMMISSION_RATE     --手续费率
    ,T.IS_CORRECT          --实付纠错
    ,T.AGENT_CODE          --业务员工号
    ,T.SEQUENCE_ID         --人管系统流水号
    ,T.BATCH_NO            --业务批次号
	    ,T.AUDIT_STATUS          --支付审核标识
    ,T.BRANCH_ID             --人员所属分公司ID
    ,T.PERIOD_TYPE           --产品期限
    ,T.GL_PRODUCT_TYPE       --产品财务大类
    ,T.DEBT_TYPE             --产品混合合同的分拆及重大保险风险测试
    ,T.BENEFIT_TYPE          --产品责任分类
    ,T.BUSI_TYPE             --险种大类代码
    ,T.INTERNAL_ID           --产品代码
    ,T.AGENCY_TYPE           --中介机构类型
    ,T.AGENT_SUB_CATE        --代理人子分类
    ,T.LABOR_FORM            --代理人用工属性
    ,T.CASH_BANK_NAME        --保险公司收付费银行名称
    ,T.SYSTEM_SOURCE         --系统来源
    ,T.TRANS_ID              --交易ID
    ,T.STATUS                --记账状态
    ,T.POLICY_AGENCY_TYPE    --保单产险/健康险销售人员的中介机构类型
    ,T.POLICY_AGENT_CATE     --保单产险/健康险销售人员的类别
    ,T.POLICY_AGENT_CODE     --保单产险/健康险销售人员
    ,T.IS_CLEAR              --是否清账
    ,T.INSERT_TIME           --记录生成日期
    ,T.UPDATE_TIME           --记录更新日期
    ,CASE WHEN T.OPER_FLAG = 'D' 
        THEN 'N' 
        ELSE 'Y' 
        END AS STATUS_FLAG  --状态
    ,TO_DATE(:P_INPUT_DATADATE) AS START_DATE  --开始时间
    ,TO_TIMESTAMP('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS END_DATE  --结束时间
    ,CURRENT_TIMESTAMP AS DP_TIME  --插入时间
FROM SRC.PROC_LI_SRC_ACC_H_T_COMMISSION_FEE_MID_TMP AS T
;
/**************插入新拉链 END**************/

/************************************主体功能代码**********END********************************************/


/*********执行成功日志******BEGIN*********/
    CALL ETL_CTRL.PROC_LI_ETL_LOG(
        P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,
        V_WF_STATUS, P_INPUT_DATADATE, '', ''
    );
    P_OUT_WF_STATUS := V_WF_STATUS;
/*********执行成功日志******END***********/

/*********执行失败日志******BEGIN*********/
EXCEPTION 
    WHEN OTHERS THEN 
        V_WF_STATUS := 'E';
        P_OUT_WF_STATUS := V_WF_STATUS;
        CALL ETL_CTRL.PROC_LI_ETL_LOG(
            P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,
            V_WF_STATUS, P_INPUT_DATADATE, SQLSTATE, LEFT(SQLERRM,2000) 
        );
/*********执行失败日志******END***************/

END;

/