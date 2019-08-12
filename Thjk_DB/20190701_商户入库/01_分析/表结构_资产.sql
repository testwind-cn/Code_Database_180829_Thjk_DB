-- 【资产】 ---------
drop table APMS_MCHT;

--==============================================================
-- Table: APMS_MCHT
--==============================================================
create table APMS_MCHT
(
   ENTITY_OID           DECIMAL(31,0)          not null,'系统参考号';
   INST_OID             DECIMAL(31,0),'机构号';
   MCHT_CD              VARCHAR(50),'商户号';
   AIP_BRAN_ID          VARCHAR(30), '分公司代码';
   "NAME"               VARCHAR(200),'商户名称';
   NAME_BUSI            VARCHAR(200),'商户营业名称';
   UP_BC_CD             VARCHAR(20),'商户收单机构代码';
   BUSI_LICE_NO         VARCHAR(80), '营业执照号码';
   UP_MCC_CD            VARCHAR(8),'MCC码';
   PROV_CD              VARCHAR(10),'省';
   CITY_CD              VARCHAR(10),'市';
   AREA_CD              VARCHAR(10),'地区';
   BUSI_ADDR            VARCHAR(200),'营业地址';
   CONTACT              VARCHAR(50),'名称';
   CONTACT_TEL          VARCHAR(100),'电话';
   FAX                  VARCHAR(20),'传真';
   EMAIL                VARCHAR(50),'电邮';
   GROUP_ID             VARCHAR(10),'集团编号';
   APPR_DATE            DATE,'商户批准日期';
   DELETE_DATE          DATE,'商户失效日期';
   DVP_BY               VARCHAR(50),'发展人或推荐人';
   STLM_INS_PROV        VARCHAR(10),'商户开户行省';
   STLM_INS_CITY        VARCHAR(10),'商户开户行市';
   STLM_ACCT            VARCHAR(50),'商户结算账号';
   STLM_NM              VARCHAR(100),'商户结算名称';
   STLM_INS_CD          VARCHAR(20),'开户行行号';
   STLM_INS_NM          VARCHAR(200),'商户开户行名称';
   STATUS               CHARACTER(1)           not null,'状态';
   REMARK               VARCHAR(500),'标志';
   VERSION              SMALLINT               not null default 0,'版本';
   WHENMODIFIED         TIMESTAMP,'更新时间';
   ORIG_FLAG            CHARACTER(1),'ORIG_FLAG';
   FLAG_DELETED         CHARACTER(1)           default 'N','删除标志';
   CREATED_BY           VARCHAR(50)            not null,'创建人';
   CREATED_DATE         TIMESTAMP              not null,'创建时间';
   LAST_UPD_BY          VARCHAR(50),'更新人';
   LAST_UPD_DATE        TIMESTAMP,'最近更新时间';
   MODI_NUM             DECIMAL(5,0),'更新次数';
   REC_DIGI_PROOF       VARCHAR(200),'REC_DIGI_PROOF';
   BROKER_NAME          VARCHAR(100),'BROKER_NAME';
   BROKER_TEL_NO        VARCHAR(32),'BROKER_TEL_NO';
   BROKER_EMAIL         VARCHAR(100),'BROKER_EMAIL';
   BROKER_IDEN_NO       VARCHAR(32),'BROKER_IDEN_NO';
   MANAGER_OID          DECIMAL(31,0),'MANAGER_OID';
   ORGANIZATION_CODE    VARCHAR(50),'ORGANIZATION_CODE';
   LEGAL_NAME           VARCHAR(50),'法定代表人姓名';
   LEGAL_IDEN_TYPE      VARCHAR(50),'法定代表人证件类型';
   LEGAL_IDEN_NO        VARCHAR(50),'法定代表人证件号码';
   SETTLEMENT_CYCLE     VARCHAR(50),'商户结算周期';
   MERCHANT_MCC         VARCHAR(10),'商户MCC码';
   NORMAL_TERM_NUM      DECIMAL(31,0),'正常终端数';
   REV_TERM_NUM         DECIMAL(31,0),'撤销终端数';
   MERCHANT_NATURE      VARCHAR(20),'商户性质';
   MERCHANT_STATUS      VARCHAR(10),'商户状态';
   LEGAL_IS_RISK        VARCHAR(2),'法人代表是否为风险商户法人代表';
   MERCHANT_DEBTS       DECIMAL(16,2),'商户负债额';
   PRINCIPAL_IS_DEBTS   VARCHAR(2),'商户负责人是否有逾期债务';
   FINANCE_NAME         VARCHAR(50),'财务联系人';
   FINANCE_HP_NO        VARCHAR(20),'财务联系人电话';
   OPEN_DATE            VARCHAR(20),'开户日期';
   TAX_NO               VARCHAR(50),'税务登记证号';
   REGISTER_ADDR        VARCHAR(200) '商户注册地址';
);
