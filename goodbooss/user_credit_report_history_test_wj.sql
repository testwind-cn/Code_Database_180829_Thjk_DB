/*
 Navicat Premium Data Transfer

 Source Server         : 172.21.12.6
 Source Server Type    : MySQL
 Source Server Version : 50636
 Source Host           : 172.21.12.6:3306
 Source Schema         : rc

 Target Server Type    : MySQL
 Target Server Version : 50636
 File Encoding         : 65001

 Date: 29/08/2018 10:29:07
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for user_credit_report_history_test_wj
-- ----------------------------
DROP TABLE IF EXISTS `user_credit_report_history_test_wj`;
CREATE TABLE `user_credit_report_history_test_wj`  (
  `tid` int(11) NOT NULL AUTO_INCREMENT,
  `user_id` int(11) NULL DEFAULT NULL,
  `upload_pboc_status` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `report_date` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `report_content` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NULL,
  `report_user_type` varchar(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `report_location` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `is_deleted` tinyint(1) NULL DEFAULT NULL,
  `create_time` datetime(0) NULL DEFAULT NULL,
  `modify_time` datetime(0) NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP(0),
  `modify_user` int(11) NULL DEFAULT NULL,
  `source` char(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `credit_content_type` char(2) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '0',
  `report_id` varchar(400) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `report_req_date` datetime(0) NULL DEFAULT NULL,
  `report_gen_date` datetime(0) NULL DEFAULT NULL,
  `report_user_name` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `report_user_id_no` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`tid`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 22577 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Compact;

SET FOREIGN_KEY_CHECKS = 1;
