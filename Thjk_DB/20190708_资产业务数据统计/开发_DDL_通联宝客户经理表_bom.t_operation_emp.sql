CREATE TABLE bom.t_operation_emp (
    `RECORD_NO` varchar(30) COLLATE utf8mb4_bin NOT NULL COMMENT '机构记录编号',
    `BRANCH_CODE` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL,
    `DEPARTMENT_CODE` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL,
    `NAME` varchar(200) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '机构全称',
    `TYPE` char(1) COLLATE utf8mb4_bin DEFAULT NULL,
    `PHONE` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
    `email` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
    `BUSI_AREA` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL,
    `CERT_TYPE` varchar(10) COLLATE utf8mb4_bin DEFAULT NULL,
    `CERT_NO` varchar(30) COLLATE utf8mb4_bin DEFAULT NULL,
    `USER_AVATAR` varchar(100) COLLATE utf8mb4_bin DEFAULT NULL,
    `USER_EMAIL` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
    `IS_AUTH_PASS` varchar(1) COLLATE utf8mb4_bin DEFAULT NULL,
    `IS_COMPLETE_SIGN` varchar(1) COLLATE utf8mb4_bin DEFAULT NULL,
    `FIELD_STATE` varchar(1) COLLATE utf8mb4_bin DEFAULT NULL,
    `IS_RECEIVE_MSG` varchar(1) COLLATE utf8mb4_bin DEFAULT NULL,
    `REFEREE_USER_ID` varchar(50) COLLATE utf8mb4_bin DEFAULT NULL,
    `STATUS` char(1) COLLATE utf8mb4_bin DEFAULT NULL COMMENT 'A-新建待审核\nB-停用待审核\nC-启用待审核\nZ-注销待审核\n0-审核未通过\n1-正常\n2-停用\n9-注销',
    /*
    A-新建待审核
    B-停用待审核
    C-启用待审核
    Z-注销待审核
    0-审核未通过
    1-正常
    2-停用
    9-注销
     */
    `CREATE_UID` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '录入人',
    `CREATE_TIME` bigint(20) DEFAULT NULL COMMENT '录入时间',
    `CHECK_UID` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '审核人',
    `CHECK_TIME` bigint(20) DEFAULT NULL COMMENT '审核时间',
    `LAST_MODIFY_UID` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '最后修改人',
    `LAST_MODIFY_TIME` bigint(20) DEFAULT NULL COMMENT '最后修改时间',
    `ARCHIVE_UID` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '归档人',
    `ARCHIVE_TIME` bigint(20) DEFAULT NULL COMMENT '归档时间',
    `ARCHIVE_FLAG` char(1) COLLATE utf8mb4_bin DEFAULT '1' COMMENT '1-未归档\r\n            2-已归档',
    `L_RECORD_NO` varchar(30) COLLATE utf8mb4_bin DEFAULT NULL COMMENT '原记录编号',
    `REMARK` text COLLATE utf8mb4_bin COMMENT '备注',
    `RESV_FLD1` text COLLATE utf8mb4_bin COMMENT '保留域1',
    `RESV_FLD2` text COLLATE utf8mb4_bin COMMENT '保留域2',
    PRIMARY KEY (`RECORD_NO`),
    KEY `index_name` (`PHONE`),
    KEY `I_t_operation_emp_EMAIL` (`email`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;